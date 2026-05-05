import { createPool } from '@vercel/postgres';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const { wd } = req.query;

  const pool = createPool();
  const { rows } = await pool.sql`
    SELECT * FROM vod 
    WHERE vod_name ILIKE ${'%' + wd + '%'} 
    LIMIT 20
  `;

  return res.json(rows);
}
