import express from "express";
import helmet from "helmet";
import morgan from "morgan";
import cors from "cors";
import { z } from "zod";
import pkg from "pg";

const { Pool } = pkg;

// --- Connect to Postgres in Docker ---
const pool = new Pool({
    host: "db", // docker-compose service name
    port: 5432,
    user: "postgres",
    password: "postgres",
    database: "postgres",
});

// --- Ensure schema on startup ---
async function ensureSchema() {
    for (let i = 0; i < 15; i++) {
        try {
            await pool.query("SELECT 1");
            break;
        } catch {
            console.log("Waiting for DB…");
            await new Promise((r) => setTimeout(r, 1000));
        }
    }

    await pool.query(`
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    CREATE TABLE IF NOT EXISTS feedback (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id TEXT NOT NULL,
      message TEXT NOT NULL,
      sentiment JSONB,
      sentiment_version TEXT,
      sentiment_updated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

const app = express();
app.use(express.static("public"));
app.use(helmet());
app.use(cors({ origin: true, credentials: true }));
app.use(express.json());
app.use(morgan("dev"));

// --- validation schema ---
const SubmitSchema = z.object({
    userId: z.string().min(1, "userId required"),
    message: z.string().min(1).max(2000),
});

// --- routes ---

app.get("/healthz", (_req, res) => res.json({ ok: true }));

// POST /feedback
app.post("/feedback", async (req, res) => {
    try {
        const { userId, message } = SubmitSchema.parse(req.body);

        const { rows } = await pool.query(
            `INSERT INTO feedback (user_id, message)
             VALUES ($1, $2)
                 RETURNING id, created_at`,
            [userId, message]
        );

        const id = rows[0].id;
        // 🔔 tell listeners a new row arrived
        await pool.query(`SELECT pg_notify('feedback_new', $1)`, [id]);

        res.status(201).json({ id, createdAt: rows[0].created_at });
    } catch (e) {
        if (e?.issues) return res.status(400).json({ error: "validation_error", details: e.issues });
        console.error(e);
        res.status(500).json({ error: "server_error" });
    }
});


// GET /feedback (public preview)
app.get("/feedback/:id", async (req, res) => {
    try {
        const { rows } = await pool.query(`
            SELECT id,
                   user_id AS "userId",
                   message,
                   sentiment->>'label' AS "sentiment",
                sentiment->>'score'  AS "score",
                created_at AS "createdAt"
            FROM feedback
            WHERE id = $1
        `, [req.params.id]);
        if (!rows.length) return res.status(404).json({ error: "not_found" });
        res.json(rows[0]);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "server_error" });
    }
});


// GET /admin/feedback?page=&pageSize=  (requires x-admin-key)
const { ADMIN_API_KEY = "supersecretadminkey" } = process.env;

app.get("/admin/feedback", async (req, res) => {
    try {
        const key = req.header("x-admin-key");
        if (key !== ADMIN_API_KEY)
            return res.status(401).json({ error: "unauthorized" });

        const page = Math.max(1, Number(req.query.page ?? 1));
        const pageSize = Math.min(50, Math.max(1, Number(req.query.pageSize ?? 10)));
        const offset = (page - 1) * pageSize;

        const { rows } = await pool.query(
            `SELECT id,
              user_id AS "userId",
              message,
              sentiment->>'label' AS "sentiment",
              sentiment->>'score' AS "score",
              created_at AS "createdAt"
       FROM feedback
       ORDER BY created_at DESC
       LIMIT $1 OFFSET $2`,
            [pageSize, offset]
        );

        const { rows: countRows } = await pool.query(
            `SELECT COUNT(*)::int AS total FROM feedback`
        );

        res.json({
            page,
            pageSize,
            total: countRows[0].total,
            data: rows,
        });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "server_error" });
    }
});

// --- Boot ---
const PORT = Number(process.env.PORT || 8080);
ensureSchema()
    .then(() => {
        app.listen(PORT, "0.0.0.0", () =>
            console.log(`API listening on http://0.0.0.0:${PORT}`)
        );
    })
    .catch((e) => {
        console.error("Failed to init schema:", e);
        process.exit(1);
    });
