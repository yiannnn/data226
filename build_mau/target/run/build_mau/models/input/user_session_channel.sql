
  create or replace   view dev.analytics.user_session_channel
  
   as (
    SELECT
    userId,
    sessionId,
    channel
FROM dev.raw_data.user_session_channel
WHERE sessionId IS NOT NULL
  );

