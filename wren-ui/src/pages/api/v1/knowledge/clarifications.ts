import { NextApiRequest, NextApiResponse } from 'next';
import { components } from '@/common';
import { ApiType } from '@server/repositories/apiHistoryRepository';
import {
  ApiError,
  handleApiError,
  respondWithSimple,
} from '@/apollo/server/utils/apiUtils';
import { getLogger } from '@server/utils';
import { persistClarificationKnowledge } from '@server/services/clarificationKnowledgeService';

const logger = getLogger('API_CLARIFICATION_KNOWLEDGE');
logger.level = 'debug';

const { projectService, instructionService, sqlPairService } = components;

interface SaveClarificationKnowledgeRequest {
  question: string;
  clarificationAnswers: Record<string, string>;
  sql?: string;
  saveInstruction?: boolean;
  saveSqlPair?: boolean;
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const startTime = Date.now();
  let project;

  try {
    project = await projectService.getCurrentProject();

    if (req.method !== 'POST') {
      throw new ApiError('Method not allowed', 405);
    }

    const {
      question,
      clarificationAnswers,
      sql,
      saveInstruction = true,
      saveSqlPair = true,
    } = req.body as SaveClarificationKnowledgeRequest;

    if (!question) {
      throw new ApiError('Question is required', 400);
    }

    if (!clarificationAnswers || !Object.keys(clarificationAnswers).length) {
      throw new ApiError('clarificationAnswers are required', 400);
    }

    const saved = await persistClarificationKnowledge(
      {
        projectId: project.id,
        question,
        clarificationAnswers,
        sql,
        saveInstruction,
        saveSqlPair,
      },
      {
        instructionRepository: components.instructionRepository,
        instructionService,
        sqlPairRepository: components.sqlPairRepository,
        sqlPairService,
      },
    );

    await respondWithSimple({
      res,
      statusCode: 201,
      responsePayload: saved,
      projectId: project.id,
      apiType: ApiType.CREATE_INSTRUCTION,
      startTime,
      requestPayload: req.body,
      headers: req.headers as Record<string, string>,
    });
  } catch (error) {
    await handleApiError({
      error,
      res,
      projectId: project?.id,
      apiType: ApiType.CREATE_INSTRUCTION,
      requestPayload: req.body,
      headers: req.headers as Record<string, string>,
      startTime,
      logger,
    });
  }
}
