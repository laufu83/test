const express = require('express');
const fetch = require('node-fetch');
const app = express();

// ==================== 改成你自己的 ====================
const SUPABASE_URL = "https://rufhslpktjrftrdrpxxi.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_A7G4cUYUB0Q9K5BN-KIwDw_APSqjUes";
// =====================================================

// 全局跨域
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  next();
});

// 1. 视频列表接口
app.get("/api/list", async (req, res) => {
  try {
    const resp = await fetch(
      `${SUPABASE_URL}/rest/v1/vod?select=id,vod_id,vod_name,vod_pic,vod_year,vod_class&order=id.desc&limit=30`,
      {
        headers: {
          apikey: SUPABASE_ANON_KEY,
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`
        }
      }
    );
    const data = await resp.json();
    res.json({ code: 200, data });
  } catch (err) {
    res.status(500).json({ code: 500, msg: err.message });
  }
});

// 2. 搜索接口 /api/search?wd=关键词
app.get("/api/search", async (req, res) => {
  const wd = req.query.wd || "";
  try {
    const resp = await fetch(
      `${SUPABASE_URL}/rest/v1/vod?select=id,vod_name,vod_pic&vod_name=ilike.*${wd}*&limit=20`,
      {
        headers: {
          apikey: SUPABASE_ANON_KEY,
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`
        }
      }
    );
    const data = await resp.json();
    res.json({ code: 200, data });
  } catch (err) {
    res.status(500).json({ code: 500, msg: err.message });
  }
});

// 启动端口 Zeabur 自动适配
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server run on ${PORT}`);
});
