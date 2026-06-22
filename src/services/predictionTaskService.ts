/**
 * Prediction Task Service
 *
 * TypeScript equivalent of the `prediction_tasks.py` module described in Issue #11.
 *
 * Architectural role:
 *   API layer  ──writes──▶  price_predictions (DB queue)  ──reads──▶  Worker
 *
 * This service is the ONLY allowed way for the API layer to interact with predictions.
 * It NEVER calls runPriceInference() directly — that boundary is enforced at import level
 * (priceReasoningEngine is not imported here).
 *
 * Job lifecycle:
 *   pending ──(worker picks up)──▶ processing ──(success)──▶ completed
 *                                                ──(error)───▶ failed
 */

import { supabase } from '@/integrations/supabase/client';
import type { PredictionStatus } from '@/integrations/supabase/types';
import type { PriceAnalysis } from './priceReasoningEngine';

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

export type { PredictionStatus };

/** Input payload needed to create a prediction job */
export interface PredictionJobInput {
  productName: string;
  location: string;
  quantity: string;
  vendorLanguage?: string;
  buyerMessage?: string;
}

/** Returned immediately after job creation (status always "pending") */
export interface PredictionJobCreated {
  job_id: string;
  status: 'pending';
  created_at: string;
}

/** Full job record — shape depends on status */
export interface PredictionJobRecord {
  job_id: string;
  status: PredictionStatus;
  symbol: string;
  location: string;
  prediction: PriceAnalysis | null;
  confidence: number | null;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  attempt_count: number;
}

/** Structured response for GET /predict/:job_id (mirrors API spec) */
export type PredictionStatusResponse =
  | { job_id: string; status: 'pending' }
  | { job_id: string; status: 'processing'; started_at: string }
  | { job_id: string; status: 'completed'; prediction: PriceAnalysis; confidence: number; timestamp: string }
  | { job_id: string; status: 'failed'; error: string };

// ─────────────────────────────────────────────────────────────────────────────
// Structured logger
// ─────────────────────────────────────────────────────────────────────────────

const logger = {
  info:  (msg: string, meta?: Record<string, unknown>) => console.info(`[PredictionTask] ${msg}`, meta ?? ''),
  warn:  (msg: string, meta?: Record<string, unknown>) => console.warn(`[PredictionTask] ${msg}`, meta ?? ''),
  error: (msg: string, meta?: Record<string, unknown>) => console.error(`[PredictionTask] ${msg}`, meta ?? ''),
};

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

function assertSupabase(): void {
  if (!supabase) {
    throw new Error('Supabase client is not initialised. Check VITE_SUPABASE_URL and VITE_SUPABASE_PUBLISHABLE_KEY.');
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Service
// ─────────────────────────────────────────────────────────────────────────────

export const predictionTaskService = {
  // ───────────────────────────────────────────────────────────────────────────
  // createJob
  //
  // Inserts a new prediction job into the queue and returns immediately.
  // No inference is performed here — the worker consumes the job asynchronously.
  //
  // Idempotency note: each call generates a fresh UUID. Callers that need
  // deduplication should check for an existing pending/processing job before
  // calling this (see findActiveJob).
  // ───────────────────────────────────────────────────────────────────────────
  async createJob(input: PredictionJobInput): Promise<PredictionJobCreated> {
    assertSupabase();

    logger.info('Creating prediction job via Edge Function', { symbol: input.productName, location: input.location });

    const { data, error } = await supabase.functions.invoke('create-prediction', {
      body: {
        productName: input.productName,
        location: input.location,
        quantity: input.quantity ?? '1 kg',
        vendorLanguage: input.vendorLanguage ?? 'hindi',
        buyerMessage: input.buyerMessage ?? null,
      },
    });

    if (error) {
      // Handle rate limit specifically
      if (error.context?.status === 429 || error.message?.includes('429')) {
        logger.warn('Rate limit exceeded for creating prediction job');
        throw new Error('Too many requests. Please try again later.');
      }
      logger.error('Failed to create prediction job via Edge Function', { error: error.message });
      throw new Error(`Failed to create prediction job: ${error.message}`);
    }

    logger.info('Prediction job created via Edge Function', { job_id: data.job_id });
    return {
      job_id:     data.job_id,
      status:     'pending',
      created_at: data.created_at,
    };
  },

  // ───────────────────────────────────────────────────────────────────────────
  // getJobStatus
  //
  // Fetches the current state of a job.
  // Returns a typed response object that mirrors the API spec shapes.
  // ───────────────────────────────────────────────────────────────────────────
  async getJobStatus(jobId: string): Promise<PredictionStatusResponse> {
    assertSupabase();

    const { data, error } = await supabase
      .from('price_predictions')
      .select('job_id, status, prediction, confidence, error_message, started_at, completed_at')
      .eq('job_id', jobId)
      .single();

    if (error) {
      logger.error('Failed to fetch job status', { job_id: jobId, error: error.message });
      throw new Error(`Job not found: ${error.message}`);
    }

    switch (data.status) {
      case 'pending':
        return { job_id: data.job_id, status: 'pending' };

      case 'processing':
        return {
          job_id:     data.job_id,
          status:     'processing',
          started_at: data.started_at ?? new Date().toISOString(),
        };

      case 'completed':
        if (!data.prediction || data.confidence == null) {
          // Guard against partial writes — treat as still processing
          return { job_id: data.job_id, status: 'processing', started_at: data.started_at ?? '' };
        }
        return {
          job_id:     data.job_id,
          status:     'completed',
          prediction: data.prediction as unknown as PriceAnalysis,
          confidence: data.confidence,
          timestamp:  data.completed_at ?? new Date().toISOString(),
        };

      case 'failed':
        return {
          job_id: data.job_id,
          status: 'failed',
          error:  data.error_message ?? 'Unknown error',
        };

      default:
        throw new Error(`Unexpected job status: ${data.status}`);
    }
  },

  // ───────────────────────────────────────────────────────────────────────────
  // getRawRecord
  //
  // Returns the full DB row (useful for admin / debugging).
  // ───────────────────────────────────────────────────────────────────────────
  async getRawRecord(jobId: string): Promise<PredictionJobRecord> {
    assertSupabase();

    const { data, error } = await supabase
      .from('price_predictions')
      .select('*')
      .eq('job_id', jobId)
      .single();

    if (error) throw new Error(`Job not found: ${error.message}`);

    return {
      job_id:        data.job_id,
      status:        data.status,
      symbol:        data.symbol,
      location:      data.location,
      prediction:    data.prediction as PriceAnalysis | null,
      confidence:    data.confidence,
      error_message: data.error_message,
      created_at:    data.created_at,
      started_at:    data.started_at,
      completed_at:  data.completed_at,
      attempt_count: data.attempt_count,
    };
  },

  // ───────────────────────────────────────────────────────────────────────────
  // findActiveJob
  //
  // Returns an existing pending/processing job for the same symbol+location,
  // enabling idempotent submission from the UI.
  // ───────────────────────────────────────────────────────────────────────────
  async findActiveJob(symbol: string, location: string): Promise<string | null> {
    assertSupabase();

    const { data } = await supabase
      .from('price_predictions')
      .select('job_id')
      .eq('symbol', symbol)
      .eq('location', location)
      .in('status', ['pending', 'processing'])
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    return data?.job_id ?? null;
  },

  // ───────────────────────────────────────────────────────────────────────────
  // subscribeToJob
  //
  // Opens a Supabase Realtime channel for a specific job row.
  // Calls onUpdate whenever the row changes (status, prediction, etc.).
  // Returns an unsubscribe function.
  //
  // This is the push-based alternative to polling — the frontend never needs
  // to poll; the DB pushes changes via WebSocket.
  // ───────────────────────────────────────────────────────────────────────────
  subscribeToJob(
    jobId: string,
    onUpdate: (record: PredictionJobRecord) => void,
  ): () => void {
    assertSupabase();

    logger.info('Subscribing to job updates', { job_id: jobId });

    const channel = supabase
      .channel(`prediction_job:${jobId}`)
      .on(
        'postgres_changes',
        {
          event:  'UPDATE',
          schema: 'public',
          table:  'price_predictions',
          filter: `job_id=eq.${jobId}`,
        },
        (payload) => {
          const row = payload.new as PredictionJobRecord;
          logger.info('Job status update received', { job_id: jobId, status: row.status });
          onUpdate(row);
        },
      )
      .subscribe((status) => {
        if (status === 'SUBSCRIBED') {
          logger.info('Realtime channel active', { job_id: jobId });
        }
      });

    // Return unsubscribe function
    return () => {
      logger.info('Unsubscribing from job updates', { job_id: jobId });
      supabase.removeChannel(channel);
    };
  },

  // ───────────────────────────────────────────────────────────────────────────
  // pollUntilComplete
  //
  // Hybrid approach: Realtime push with HTTP polling fallback.
  // Resolves when the job reaches 'completed' or 'failed'.
  // Rejects if maxWaitMs is exceeded.
  // ───────────────────────────────────────────────────────────────────────────
  async pollUntilComplete(
    jobId: string,
    onUpdate?: (status: PredictionStatusResponse) => void,
    maxWaitMs = 30_000,
  ): Promise<PredictionStatusResponse> {
    return new Promise((resolve, reject) => {
      let settled = false;
      let intervalId: ReturnType<typeof setInterval>;

      const settle = (result: PredictionStatusResponse | Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeoutId);
        clearInterval(intervalId);
        unsubscribe();
        if (result instanceof Error) reject(result);
        else resolve(result);
      };

      // Timeout guard
      const timeoutId = setTimeout(() => {
        settle(new Error(`Job ${jobId} did not complete within ${maxWaitMs}ms`));
      }, maxWaitMs);

      // Realtime push
      const unsubscribe = this.subscribeToJob(jobId, (row) => {
        if (row.status === 'completed' || row.status === 'failed') {
          this.getJobStatus(jobId).then(settle).catch(settle);
        } else {
          this.getJobStatus(jobId).then((s) => onUpdate?.(s)).catch(() => {});
        }
      });

      // Polling fallback (every 2 s) in case Realtime is unavailable
      intervalId = setInterval(async () => {
        try {
          const status = await this.getJobStatus(jobId);
          onUpdate?.(status);
          if (status.status === 'completed' || status.status === 'failed') {
            settle(status);
          }
        } catch (err) {
          logger.warn('Polling error', { error: String(err) });
        }
      }, 2000);
    });
  },
};
