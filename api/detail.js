import { createPool } from '@vercel/postgres';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const { id } = req.query;

  if (!id) return res.status(400).json({ error: 'id 不能为空' });

  const pool = createPool();
  const { rows } = await pool.sql`
    SELECT * FROM vod WHERE id = ${id}
  `;

  return res.json(rows[0] || null);
}
