-- =============================================================================
-- Migration: Revoke INSERT on price_predictions
-- Description: Drops the public INSERT policy to enforce that clients must
-- route through the `create-prediction` Edge Function for rate limiting.
-- =============================================================================

-- Drop the existing INSERT policy that allows users to create jobs directly
DROP POLICY IF EXISTS "Users can create prediction jobs" ON public.price_predictions;

-- Note: RLS is already enabled and there are no other INSERT policies for
-- anon or authenticated users. The service_role (used by the Edge Function)
-- naturally bypasses RLS and can still perform INSERTs.
