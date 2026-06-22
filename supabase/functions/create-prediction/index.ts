import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import { Ratelimit } from 'npm:@upstash/ratelimit';
import { Redis } from 'npm:@upstash/redis';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

Deno.serve(async (req: Request) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method Not Allowed' }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  try {
    // 1. Initialize Supabase client to extract user context
    const authHeader = req.headers.get('Authorization');
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? '';
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY') ?? '';

    let userId: string | null = null;
    if (authHeader) {
      const supabaseClient = createClient(supabaseUrl, supabaseAnonKey, {
        global: { headers: { Authorization: authHeader } },
      });
      const { data: { user }, error: authError } = await supabaseClient.auth.getUser();
      if (!authError && user) {
        userId = user.id;
      }
    }

    // 2. Identify the client for Rate Limiting
    // Prefer auth.uid() if present, fallback to IP address
    const ipAddress = req.headers.get('x-forwarded-for') || req.headers.get('x-real-ip') || 'anonymous_ip';
    const identifier = userId ? `user:${userId}` : `ip:${ipAddress}`;

    // 3. Configure Token Bucket Rate Limiting
    const rateLimitMax = parseInt(Deno.env.get('RATE_LIMIT_MAX') || '100', 10);
    const redis = new Redis({
      url: Deno.env.get('UPSTASH_REDIS_REST_URL')!,
      token: Deno.env.get('UPSTASH_REDIS_REST_TOKEN')!,
    });

    const ratelimit = new Ratelimit({
      redis,
      limiter: Ratelimit.tokenBucket(rateLimitMax, '1 m', rateLimitMax),
      analytics: false,
      prefix: 'ratelimit:create_prediction',
    });

    const { success, limit, remaining, reset } = await ratelimit.limit(identifier);

    // 4. Enforce Rate Limit
    if (!success) {
      return new Response(
        JSON.stringify({
          error: 'Too many requests. Please try again later.',
          rateLimit: { limit, remaining, reset },
        }),
        {
          status: 429,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        }
      );
    }

    // 5. Parse payload
    const body = await req.json();
    const { productName, location, quantity, vendorLanguage, buyerMessage } = body;

    if (!productName || !location) {
      return new Response(
        JSON.stringify({ error: 'productName and location are required' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // 6. Secure Insertion (bypass RLS using service_role key)
    const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '';
    const serviceClient = createClient(supabaseUrl, serviceRoleKey);

    const { data, error } = await serviceClient
      .from('price_predictions')
      .insert({
        symbol: productName,
        location: location,
        quantity: quantity || '1 kg',
        vendor_language: vendorLanguage || 'hindi',
        buyer_message: buyerMessage || null,
        status: 'pending',
        user_id: userId, // associate with user if authenticated
      })
      .select('job_id, created_at')
      .single();

    if (error) {
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    // 7. Success response
    return new Response(JSON.stringify(data), {
      status: 201,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (error: unknown) {
    const errMessage = error instanceof Error ? error.message : 'Unknown error';
    return new Response(JSON.stringify({ error: errMessage }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
