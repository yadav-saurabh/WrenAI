import re
from dataclasses import dataclass


@dataclass
class ClarificationQuestion:
    id: str
    question: str
    options: list[str] | None = None
    reason: str | None = None


@dataclass
class ClarificationResult:
    needs_clarification: bool
    questions: list[ClarificationQuestion]


@dataclass
class ValidationResult:
    valid: bool
    violations: list[str]
    clarification_questions: list[ClarificationQuestion]
    fallback_sql: str | None = None
    fallback_question: str | None = None


FINPOWER_BROAD_HINTS = (
    "investor", "investors", "client", "clients", "broker", "brokers", "loan", "loans", "email", "investment value",
)


def _norm(text: str | None) -> str:
    return (text or "").strip().lower()


def _question_mentions_finpower_business_logic(question: str) -> bool:
    q = _norm(question)
    return any(token in q for token in FINPOWER_BROAD_HINTS)


def detect_clarifications(
    question: str,
    clarification_answers: dict[str, str] | None = None,
) -> ClarificationResult:
    q = _norm(question)
    answers = {k: _norm(v) for k, v in (clarification_answers or {}).items() if v}
    questions: list[ClarificationQuestion] = []

    if _question_mentions_finpower_business_logic(q):
        if (
            ("investor" in q or "client" in q)
            and "population_definition" not in answers
            and "active individual" not in q
            and "active individual investors" not in q
        ):
            questions.append(
                ClarificationQuestion(
                    id="population_definition",
                    question="When you say investors/clients, should I use only active individual investor clients, or all client records?",
                    options=[
                        "Only active individual investor clients",
                        "All client records",
                    ],
                    reason="The business population can materially change the answer.",
                )
            )

        if (
            "investment" in q
            and "value" in q
            and "investment_value_definition" not in answers
            and "current balance" not in q
            and "current value" not in q
            and "transaction" not in q
        ):
            questions.append(
                ClarificationQuestion(
                    id="investment_value_definition",
                    question="For investment value, do you want the current investment balance/value, or the total of investment transactions?",
                    options=[
                        "Current investment balance/value",
                        "Total of investment transactions",
                    ],
                    reason="Transaction totals and current balances are different business measures.",
                )
            )

        if (
            ("not loan" in q or "not loans" in q)
            and "loan_exclusion_definition" not in answers
        ):
            questions.append(
                ClarificationQuestion(
                    id="loan_exclusion_definition",
                    question="Should I exclude clients linked to loan accounts as borrowers, guarantors, or co-borrowers?",
                    options=[
                        "Yes, exclude clients linked to loan accounts in those roles",
                        "No, only exclude loan-type client records",
                    ],
                    reason="There are multiple ways to interpret 'not loans'.",
                )
            )

    deduped: list[ClarificationQuestion] = []
    seen = set()
    for question in questions:
        if question.id not in seen:
            deduped.append(question)
            seen.add(question.id)

    return ClarificationResult(
        needs_clarification=bool(deduped),
        questions=deduped,
    )


def apply_clarification_answers(question: str, clarification_answers: dict[str, str] | None) -> str:
    if not clarification_answers:
        return question

    lines = [question, "", "Clarification answers confirmed by the user:"]
    for key, value in clarification_answers.items():
        if value:
            lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def pick_best_sql_pair(sql_samples: list[dict] | None) -> tuple[str | None, str | None]:
    if not sql_samples:
        return None, None

    top = sql_samples[0] or {}
    return top.get("sql"), top.get("question")


_CLIENT_TYPE_BROKER_PATTERN = re.compile(r"client_type_id\s*(=|<>|!=)\s*'?broker'?", re.I)
_CLIENT_TYPE_LOAN_PATTERN = re.compile(r"client_type_id\s*(=|<>|!=)\s*'?loan'?", re.I)
_DIRECT_CLIENT_IDS_JOIN_PATTERN = re.compile(r'client_ids\s*=\s*[\w\."]*client_id', re.I)
_SUM_ALL_ACCOUNT_TX_VALUES_PATTERN = re.compile(
    r"sum\s*\(\s*cast\s*\([^\)]*account_transactions[^\)]*value", re.I | re.S
)


def validate_generated_sql(
    question: str,
    sql: str,
    sql_samples: list[dict] | None = None,
) -> ValidationResult:
    q = _norm(question)
    normalized_sql = _norm(sql)
    violations: list[str] = []
    clarification_questions: list[ClarificationQuestion] = []

    if _question_mentions_finpower_business_logic(q):
        if _CLIENT_TYPE_BROKER_PATTERN.search(normalized_sql):
            violations.append(
                "Broker exclusion is using client_type_id =/<> 'broker', but this database uses coded client types and broker logic should come from broker employment role/business semantics."
            )

        if _CLIENT_TYPE_LOAN_PATTERN.search(normalized_sql):
            violations.append(
                "Loan exclusion is using client_type_id =/<> 'loan', but loan membership should come from loan-account relationships/business semantics."
            )

        if _DIRECT_CLIENT_IDS_JOIN_PATTERN.search(normalized_sql):
            violations.append(
                "The SQL joins accounts.client_ids directly to client_id, which is unsafe for multi-client accounts."
            )

        if "not broker" in q or "not brokers" in q:
            if 'role_type' not in normalized_sql and 'broker' in normalized_sql:
                violations.append(
                    "The SQL mentions broker filtering but does not appear to use broker employment role logic."
                )

        if "not loan" in q or "not loans" in q:
            if 'product_type' not in normalized_sql:
                violations.append(
                    "The SQL does not appear to exclude loan-linked accounts via product type logic."
                )

        if "email" in q and 'contact_type' in normalized_sql and 'is_current' not in normalized_sql and 'contact_method_description' not in normalized_sql:
            violations.append(
                "Email detection appears too weak; current/preferred email semantics are missing."
            )

        if "investment" in q and "total investments" in q and _SUM_ALL_ACCOUNT_TX_VALUES_PATTERN.search(normalized_sql):
            violations.append(
                "Investment value appears to be based on raw transaction sums instead of a scoped investment-value definition."
            )

        if ("investor" in q or "investors/clients" in q) and 'client_type_id' not in normalized_sql:
            clarification_questions.append(
                ClarificationQuestion(
                    id="population_definition",
                    question="Should I use only active individual investor clients, or all client records?",
                    options=[
                        "Only active individual investor clients",
                        "All client records",
                    ],
                )
            )

    fallback_sql, fallback_question = pick_best_sql_pair(sql_samples)
    return ValidationResult(
        valid=not violations,
        violations=violations,
        clarification_questions=clarification_questions,
        fallback_sql=fallback_sql,
        fallback_question=fallback_question,
    )
