import os, time, json, select
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

PG_DSN = "dbname=postgres user=postgres password=postgres host=db port=5432"
analyzer = SentimentIntensityAnalyzer()

def get_conn():
    return psycopg2.connect(PG_DSN)

def classify(text: str):
    vs = analyzer.polarity_scores(text or "")
    score = vs["compound"]
    label = "positive" if score >= 0.05 else "negative" if score <= -0.05 else "neutral"
    return {"score": score, "label": label, "model": "vader-0.1"}

def run_once(max_rows=100):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
                    SELECT id, message
                    FROM feedback
                    WHERE sentiment IS NULL
                    ORDER BY created_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    """, (max_rows,))
        rows = cur.fetchall()
        if not rows:
            print("No pending rows; sleeping…", flush=True)
            return 0
        print(f"Processing {len(rows)} rows…", flush=True)
        for fid, message in rows:
            s = classify(message)
            cur.execute("""
                        UPDATE feedback
                        SET sentiment = %s::jsonb,
                  sentiment_version = %s,
                            sentiment_updated_at = now()
                        WHERE id = %s
                        """, (json.dumps(s), s["model"], fid))
        print("Updated sentiments.", flush=True)
        return len(rows)

def start_listener():
    # autocommit is required for LISTEN/NOTIFY to receive notifications
    conn = psycopg2.connect(PG_DSN)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("LISTEN feedback_new;")
    print("Listening on channel 'feedback_new'…", flush=True)
    return conn

if __name__ == "__main__":
    # first sweep any historical rows
    run_once()
    # then listen for new inserts and react immediately
    lconn = start_listener()
    while True:
        # wait up to 5s for a notify; if none, do a quick sweep
        if select.select([lconn], [], [], 5) == ([], [], []):
            run_once()
            continue
        lconn.poll()
        while lconn.notifies:
            note = lconn.notifies.pop(0)
            # optional: payload is the id -> could target only that id
            # for simplicity, just run a small sweep
            run_once(max_rows=50)
