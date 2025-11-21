
import { serve } from "https://deno.land/std@0.182.0/http/server.ts";
import { DB } from "https://deno.land/x/sqlite@v3.9.1/mod.ts";

const CONFIG = {
    token: Deno.env.get("FACTORY_AI_TOKEN") ?? "",
    apiUrl: "https://app.factory.ai/api/organization/members/chat-usage",
    dbFile: "usage_data.db",
    interval: 10 * 1000, // 10s
};

if (!CONFIG.token) {
    console.error("Missing FACTORY_AI_TOKEN environment variable.");
    Deno.exit(1);
}

// 初始化 SQLite 数据库
const db = new DB(CONFIG.dbFile);
db.execute(`
  CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    user_tokens INTEGER,
    org_tokens INTEGER,
    total_allowance INTEGER,
    raw_data TEXT
  )
`);

// 抓取数据函数
async function fetchAndSave(force = false) {
    try {
        // console.log(`[${new Date().toLocaleTimeString()}] Fetching...`);
        const res = await fetch(CONFIG.apiUrl, {
            headers: {
                "Authorization": `Bearer ${CONFIG.token}`,
                "Content-Type": "application/json"
            }
        });
        
        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const data = await res.json();
        const usage = data.usage.standard; // 提取关键数据

        // 检查上一条记录
        const [lastRow] = db.query("SELECT user_tokens, timestamp FROM records ORDER BY id DESC LIMIT 1");
        
        let shouldSave = true;
        if (!force && lastRow) {
            const [lastTokens, lastTs] = lastRow;
            // 如果 Token 数没变
            if (lastTokens === usage.userTokens) {
                // 只有距离上次保存超过 60 秒才存 (心跳机制)
                if (Date.now() - (lastTs as number) < 60 * 1000) {
                    shouldSave = false;
                    console.log(`💤 No change. Skip.`);
                } else {
                    console.log(`💓 Heartbeat save.`);
                }
            }
        }

        if (shouldSave) {
            db.query(
                "INSERT INTO records (timestamp, user_tokens, org_tokens, total_allowance, raw_data) VALUES (?, ?, ?, ?, ?)",
                [Date.now(), usage.userTokens, usage.orgTotalTokensUsed, usage.totalAllowance, JSON.stringify(data)]
            );
            console.log(`✅ Data saved. User: ${usage.userTokens}`);
        }

    } catch (error) {
        console.error("❌ Error fetching data:", error.message);
    }
}

// 定时任务
setInterval(() => fetchAndSave(false), CONFIG.interval);
// 启动时强制写入一次，确保有数据
fetchAndSave(true);

// Web 服务器
async function handler(req: Request): Promise<Response> {
    const url = new URL(req.url);
    
    // API: 获取最新一条数据
    if (url.pathname === "/api/latest") {
        try {
            const [row] = db.query("SELECT * FROM records ORDER BY id DESC LIMIT 1");
            if (row) {
                const [id, timestamp, userTokens, orgTokens, allowance, raw] = row;
                return new Response(JSON.stringify({
                    timestamp,
                    userTokens,
                    orgTokens,
                    allowance,
                    raw: JSON.parse(raw as string)
                }), { headers: { "Content-Type": "application/json" } });
            }
            return new Response("{}", { headers: { "Content-Type": "application/json" } });
        } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), { status: 500 });
        }
    }

    // API: 获取历史趋势
    if (url.pathname === "/api/history") {
        try {
            const rows = [...db.query("SELECT timestamp, user_tokens FROM records ORDER BY id DESC LIMIT 1440")];
            const data = rows.reverse().map(([ts, val]) => ({ timestamp: ts, value: val }));
            return new Response(JSON.stringify(data), { headers: { "Content-Type": "application/json" } });
        } catch (e) {
            return new Response("[]", { headers: { "Content-Type": "application/json" } });
        }
    }

    // 静态页面
    if (url.pathname === "/" || url.pathname === "/index.html") {
        try {
            const html = await Deno.readTextFile("index.html");
            return new Response(html, { headers: { "Content-Type": "text/html" } });
        } catch {
            return new Response("index.html not found", { status: 404 });
        }
    }

    return new Response("Not Found", { status: 404 });
}

console.log("🚀 Server running on http://localhost:12345");
await serve(handler, { port: 12345 });
