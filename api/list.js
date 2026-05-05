import { createPool } from '@vercel/postgres';

export default async function handler(req, res) {
  // 跨域
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const pool = createPool();
  const limit = req.query.limit || 30;

  try {
    const { rows } = await pool.sql`
      SELECT id, vod_id, vod_name, vod_pic, vod_year, vod_class 
      FROM vod 
      ORDER BY id DESC 
      LIMIT ${limit}
    `;

    return res.status(200).json({
      code: 200,
      data: rows,
      total: rows.length
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
