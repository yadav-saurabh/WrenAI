import { getLogger } from '@server/utils';
import { IInstructionRepository, Instruction } from '@server/repositories';
import {
  ISqlPairRepository,
  SqlPair,
} from '@server/repositories/sqlPairRepository';
import { IInstructionService } from './instructionService';
import { ISqlPairService } from './sqlPairService';

const logger = getLogger('ClarificationKnowledgeService');

export interface PersistClarificationKnowledgeInput {
  projectId: number;
  question?: string;
  clarificationAnswers?: Record<string, string>;
  sql?: string;
  saveInstruction?: boolean;
  saveSqlPair?: boolean;
}

interface PersistClarificationKnowledgeDependencies {
  instructionRepository: IInstructionRepository;
  instructionService: IInstructionService;
  sqlPairRepository: ISqlPairRepository;
  sqlPairService: ISqlPairService;
}

export const buildClarificationInstruction = (
  question: string,
  clarificationAnswers: Record<string, string>,
): string => {
  const clarificationSummary = Object.entries(clarificationAnswers)
    .filter(([, value]) => !!value)
    .map(([key, value]) => `- ${key}: ${value}`)
    .join('\n');

  return [
    `When answering questions similar to: "${question}", use these confirmed business meanings:`,
    clarificationSummary,
  ]
    .filter(Boolean)
    .join('\n');
};

export const persistClarificationKnowledge = async (
  {
    projectId,
    question,
    clarificationAnswers,
    sql,
    saveInstruction = true,
    saveSqlPair = true,
  }: PersistClarificationKnowledgeInput,
  {
    instructionRepository,
    instructionService,
    sqlPairRepository,
    sqlPairService,
  }: PersistClarificationKnowledgeDependencies,
) => {
  if (!question || !clarificationAnswers) {
    return {
      savedInstruction: false,
      savedSqlPair: false,
      instruction: null as Instruction | null,
      sqlPair: null as SqlPair | null,
    };
  }

  const instruction = buildClarificationInstruction(
    question,
    clarificationAnswers,
  );

  let savedInstruction = false;
  let instructionRecord: Instruction | null = null;
  if (saveInstruction && instruction) {
    const existingInstructions = await instructionRepository.findAllBy({
      projectId,
    });
    const hasInstruction = existingInstructions.some(
      (item) => item.instruction === instruction,
    );

    if (!hasInstruction) {
      try {
        instructionRecord = await instructionService.createInstruction({
          instruction,
          questions: [question],
          isDefault: false,
          projectId,
        });
        savedInstruction = true;
      } catch (error) {
        logger.warn(`Failed to persist clarification instruction: ${error}`);
      }
    }
  }

  let savedSqlPair = false;
  let sqlPairRecord: SqlPair | null = null;
  if (saveSqlPair && sql) {
    const hasSqlPair = !!(await sqlPairRepository.findOneBy({
      projectId,
      question,
      sql,
    }));

    if (!hasSqlPair) {
      try {
        sqlPairRecord = await sqlPairService.createSqlPair(projectId, {
          question,
          sql,
        });
        savedSqlPair = true;
      } catch (error) {
        logger.warn(`Failed to persist clarification SQL pair: ${error}`);
      }
    }
  }

  return {
    savedInstruction,
    savedSqlPair,
    instruction: instructionRecord,
    sqlPair: sqlPairRecord,
  };
};
