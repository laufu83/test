import { createPool } from '@vercel/postgres';

export default async function handler(req, res) {
  // 跨域
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST');

  // 测试：先不连数据库，看能不能打开
  return res.status(200).json({
    message: "✅ 接口部署成功！",
    success: true
  });
}
