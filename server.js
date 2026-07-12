/**
 * Weazel News — Majestic RP
 * Render.com + Neon PostgreSQL (без карты)
 */
'use strict';
require('dotenv').config();

const express      = require('express');
const session      = require('express-session');
const pgSession    = require('connect-pg-simple')(session);
const rateLimit    = require('express-rate-limit');
const helmet       = require('helmet');
const bcrypt       = require('bcrypt');
const { v4: uuid } = require('uuid');
const { Pool }     = require('pg');
const path         = require('path');
const fs           = require('fs');
const crypto       = require('crypto');
const multer       = require('multer');
const cloudinary   = require('cloudinary').v2;
// sharp используется ТОЛЬКО для необязательного сжатия картинок перед
// загрузкой в Cloudinary (см. shrinkImageIfNeeded ниже). Грузим его
// защищённо: если на конкретной платформе/архитектуре не нашлось
// подходящего нативного бинарника и require бросает исключение — сайт
// всё равно должен подняться и работать, просто без автосжатия (файлы
// больше 10 МБ в этом случае будут отклоняться Cloudinary как раньше).
let sharp = null;
try { sharp = require('sharp'); }
catch (e) { console.warn('ВНИМАНИЕ: модуль sharp не загрузился — автосжатие изображений перед Cloudinary отключено:', e.message); }

const PORT           = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(64).toString('hex');
const BCRYPT_ROUNDS  = 12;
const DATABASE_URL   = process.env.DATABASE_URL;
const IS_PROD        = process.env.NODE_ENV === 'production';

if (!DATABASE_URL) {
  console.error('ОШИБКА: DATABASE_URL не задан!');
  process.exit(1);
}

const UPLOADS_DIR = path.join(__dirname, 'uploads');
fs.mkdirSync(UPLOADS_DIR, { recursive: true });

// ─── CLOUDINARY: постоянное хранилище для загруженных картинок ───────────
// Render/Fly пересоздают диск контейнера при каждом деплое — всё, что
// сохранено локально в UPLOADS_DIR, пропадает. Поэтому все загрузки
// (фоны страниц, аватарки участников состава, картинки новостей) теперь
// уходят в Cloudinary — облако, независимое от деплоя сайта.
// Нужны 3 переменные окружения: CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY,
// CLOUDINARY_API_SECRET (см. Dashboard → Product Environment Credentials
// на cloudinary.com). Если их нет — сайт продолжит работать, но упадёт
// обратно на локальный диск (только для локальной разработки: на проде
// без этих переменных загруженные файлы будут теряться при редеплое).
const CLOUDINARY_CLOUD_NAME = process.env.CLOUDINARY_CLOUD_NAME || '';
const CLOUDINARY_API_KEY    = process.env.CLOUDINARY_API_KEY || '';
const CLOUDINARY_API_SECRET = process.env.CLOUDINARY_API_SECRET || '';
const CLOUDINARY_ENABLED    = !!(CLOUDINARY_CLOUD_NAME && CLOUDINARY_API_KEY && CLOUDINARY_API_SECRET);

if (CLOUDINARY_ENABLED) {
  cloudinary.config({
    cloud_name: CLOUDINARY_CLOUD_NAME,
    api_key: CLOUDINARY_API_KEY,
    api_secret: CLOUDINARY_API_SECRET,
    secure: true,
  });
  console.log('Cloudinary подключён — загруженные файлы переживут редеплой');
} else {
  console.warn('ВНИМАНИЕ: Cloudinary не настроен (нет CLOUDINARY_CLOUD_NAME / CLOUDINARY_API_KEY / CLOUDINARY_API_SECRET). Загруженные через сайт картинки (фоны, аватарки, изображения новостей) будут теряться при каждом редеплое!');
}

function uploadBufferToCloudinary(buffer) {
  return new Promise((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      { folder: 'weazel-news', resource_type: 'image' },
      (err, result) => { if (err) reject(err); else resolve(result); }
    );
    stream.end(buffer);
  });
}

// Бесплатный план Cloudinary принимает картинку не тяжелее 10 МБ
// (10 485 760 байт) — а фото с телефона/скриншоты часто крупнее. Чтобы
// загрузка не падала с «File size too large», перед отправкой в Cloudinary
// пережимаем файл: сначала аккуратно уменьшаем разрешение, если оно
// избыточно для сайта, затем при необходимости постепенно снижаем
// качество/разрешение ещё, пока не впишемся в лимит (максимум 6 попыток).
// GIF не трогаем вообще — пересжатие сломало бы анимацию.
const CLOUDINARY_MAX_BYTES = 9.5*1024*1024; // небольшой запас под лимит в 10 МБ
async function shrinkImageIfNeeded(buffer, mimetype) {
  if (!sharp || buffer.length <= CLOUDINARY_MAX_BYTES || mimetype === 'image/gif') return buffer;
  try {
    const meta = await sharp(buffer).metadata();
    const format = meta.format === 'png' ? 'png' : meta.format === 'webp' ? 'webp' : 'jpeg';
    let width = meta.width || 2600;
    let quality = 85;
    let out = buffer;
    for (let i = 0; i < 6; i++) {
      let pipeline = sharp(buffer, { failOn: 'none' }).rotate(); // rotate() без аргументов — учитывает EXIF-ориентацию
      if (width < (meta.width || width)) pipeline = pipeline.resize({ width, withoutEnlargement: true });
      if (format === 'png') out = await pipeline.png({ quality, compressionLevel: 9, palette: true }).toBuffer();
      else if (format === 'webp') out = await pipeline.webp({ quality }).toBuffer();
      else out = await pipeline.jpeg({ quality, mozjpeg: true }).toBuffer();
      if (out.length <= CLOUDINARY_MAX_BYTES) break;
      quality = Math.max(40, quality - 12);
      width = Math.round(width * 0.85);
    }
    return out.length < buffer.length ? out : buffer;
  } catch (e) {
    console.error('Не удалось сжать изображение перед загрузкой в Cloudinary:', e.message);
    return buffer; // не получилось сжать — пробуем отправить как есть
  }
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

async function query(sql, params = []) {
  const client = await pool.connect();
  try { return await client.query(sql, params); }
  finally { client.release(); }
}

async function initDB() {
  await query(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE,
      pwd_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'guest'
        CHECK(role IN ('guest','editor','admin')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_login TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS news (
      id TEXT PRIMARY KEY, title TEXT NOT NULL, category TEXT DEFAULT '',
      excerpt TEXT DEFAULT '', blocks TEXT DEFAULT '[]',
      img TEXT DEFAULT '', bg_img TEXT DEFAULT '', align TEXT DEFAULT 'left',
      title_color TEXT DEFAULT '', text_color TEXT DEFAULT '',
      author_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      author_name TEXT DEFAULT 'Редакция',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS services (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, items TEXT DEFAULT '[]', sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS team_cats (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, layout TEXT DEFAULT 'pyramid', sort_order INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS team_members (
      id TEXT PRIMARY KEY, cat_id TEXT REFERENCES team_cats(id) ON DELETE CASCADE,
      name TEXT NOT NULL, role TEXT DEFAULT '', photo TEXT DEFAULT '', sort_order INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS visitors (
      id SERIAL PRIMARY KEY, user_name TEXT DEFAULT 'Гость',
      page TEXT DEFAULT '', ip_hash TEXT DEFAULT '',
      visited_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    -- Статистика посещений сайта: 1 запись = 1 уникальное устройство за 1 день
    -- (UNIQUE(visitor_id,visit_date) + ON CONFLICT DO NOTHING при записи).
    -- Не путать с таблицей visitors выше — там сырой журнал КАЖДОГО перехода
    -- между разделами сайта, здесь — дедуплицированные посещения для статистики.
    CREATE TABLE IF NOT EXISTS site_visits (
      id TEXT PRIMARY KEY, visitor_id TEXT NOT NULL,
      visit_date DATE NOT NULL, ip_hash TEXT DEFAULT '',
      first_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(visitor_id, visit_date)
    );
    CREATE INDEX IF NOT EXISTS idx_site_visits_date ON site_visits(visit_date);
    -- Журнал редактирования полей: 1 запись = 1 сохранение с массивом
    -- изменённых полей {field, before, after}. Видно только Администратору
    -- (см. requireAdmin на роуте /api/edit-logs) — роль Leader сюда доступа
    -- не имеет, как и к /api/site-visits/stats.
    CREATE TABLE IF NOT EXISTS edit_logs (
      id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      user_name TEXT DEFAULT 'Система', entity TEXT NOT NULL, entity_id TEXT DEFAULT '',
      entity_label TEXT DEFAULT '', changes JSONB NOT NULL DEFAULT '[]',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_edit_logs_created ON edit_logs(created_at DESC);
    CREATE TABLE IF NOT EXISTS site_settings (key TEXT PRIMARY KEY, value TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS login_attempts (
      ip_hash TEXT PRIMARY KEY, count INTEGER DEFAULT 0, locked_until TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS session (
      sid TEXT PRIMARY KEY, sess JSONB NOT NULL, expire TIMESTAMPTZ NOT NULL
    );
    CREATE INDEX IF NOT EXISTS session_expire ON session(expire);
  `);

  // Миграция: добавить новые колонки если БД уже существует
  await query(`ALTER TABLE news ADD COLUMN IF NOT EXISTS title_color TEXT DEFAULT ''`);
  await query(`ALTER TABLE news ADD COLUMN IF NOT EXISTS text_color TEXT DEFAULT ''`);

  // Миграция: добавить роли 'advertising' (Advertising Department) и 'curator_ad' (Curator AD)
  // + 'leader' (Лидер — доступ как у Администратора, кроме статистики
  // посещений и журнала редактирования, см. requireAdmin ниже).
  await query(`ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check`);
  await query(`ALTER TABLE users ADD CONSTRAINT users_role_check CHECK(role IN ('guest','editor','admin','advertising','curator_ad','leader'))`);

  // ─── Модуль «Контракты» (роли, таблица контрактов, калькулятор, статистика) ───
  await query(`
    CREATE TABLE IF NOT EXISTS employees (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, static_id TEXT DEFAULT '',
      active BOOLEAN NOT NULL DEFAULT true, sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS contract_slots (
      id TEXT PRIMARY KEY,
      color TEXT NOT NULL CHECK(color IN ('green','red')),
      slot_date DATE NOT NULL,
      slot_time TEXT NOT NULL,
      status BOOLEAN NOT NULL DEFAULT false,
      price NUMERIC NOT NULL DEFAULT 0,
      text TEXT DEFAULT '',
      accepted_id TEXT REFERENCES employees(id) ON DELETE SET NULL,
      declined_id TEXT REFERENCES employees(id) ON DELETE SET NULL,
      payout NUMERIC NOT NULL DEFAULT 0,
      transfer_time TEXT DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(color, slot_date, slot_time)
    );
    CREATE INDEX IF NOT EXISTS idx_contract_slots_date ON contract_slots(slot_date);
    CREATE TABLE IF NOT EXISTS bonuses (
      id TEXT PRIMARY KEY,
      employee_id TEXT REFERENCES employees(id) ON DELETE CASCADE,
      week_start DATE NOT NULL,
      amount NUMERIC NOT NULL DEFAULT 0,
      comment TEXT DEFAULT '',
      paid BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_bonuses_week ON bonuses(week_start);
  `);

  // Миграция: шрифт для описания (должности) участника состава
  await query(`ALTER TABLE team_members ADD COLUMN IF NOT EXISTS role_font TEXT DEFAULT ''`);

  // Миграция: дата создания категории услуг (нужна для защиты от дублей при двойной отправке формы)
  await query(`ALTER TABLE services ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`);

  // Миграция: если sort_order ещё не проставлен (старые данные до этой версии) —
  // заполняем его на основе текущего физического порядка строк (ctid), чтобы
  // кнопки "переместить вверх/вниз" сразу заработали на уже существующих данных.
  // На новых записях sort_order выставляется явно при создании — эта миграция
  // их не трогает.
  await query(`
    UPDATE team_members m SET sort_order = sub.rn
    FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY cat_id ORDER BY ctid) AS rn FROM team_members) sub
    WHERE m.id = sub.id AND m.sort_order = 0
  `);
  await query(`
    UPDATE team_cats c SET sort_order = sub.rn
    FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY ctid) AS rn FROM team_cats) sub
    WHERE c.id = sub.id AND c.sort_order = 0
  `);

  const adminEmail = process.env.ADMIN_EMAIL || 'computer52552@gmail.com';
  const adminPass  = process.env.ADMIN_PASSWORD || '098456964';
  const adminName  = process.env.ADMIN_NAME || 'degrees';
  const ex = await query('SELECT id FROM users WHERE email=$1', [adminEmail.toLowerCase()]);
  if (!ex.rows.length) {
    const hash = await bcrypt.hash(adminPass, BCRYPT_ROUNDS);
    await query('INSERT INTO users (id,name,email,pwd_hash,role) VALUES ($1,$2,$3,$4,$5)',
      [uuid(), adminName, adminEmail.toLowerCase(), hash, 'admin']);
    console.log('Администратор создан:', adminEmail);
  }
  console.log('База данных готова');
}

const app = express();
app.set('trust proxy', 1);

app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'","'unsafe-inline'"],
      // ВАЖНО: script-src-attr — отдельная директива от script-src.
      // Helmet по умолчанию ставит её в 'none', что блокирует ВСЕ
      // onclick="..." и подобные атрибуты, даже если script-src разрешает
      // unsafe-inline. Наш сайт построен на onclick-атрибутах — разрешаем явно.
      scriptSrcAttr: ["'unsafe-inline'"],
      styleSrc: ["'self'","'unsafe-inline'","https://fonts.googleapis.com"],
      styleSrcAttr: ["'unsafe-inline'"],
      fontSrc: ["'self'","https://fonts.gstatic.com"],
      imgSrc: ["'self'","data:","blob:","https:","http:"],
      frameSrc: ["'self'","https://online.fliphtml5.com","https://www.youtube.com","https://player.vimeo.com"],
      connectSrc: ["'self'"],
    }
  }, crossOriginEmbedderPolicy: false,
}));

app.use(express.json({ limit: '20mb' }));
app.use(express.urlencoded({ extended: true, limit: '20mb' }));

app.use(session({
  store: new pgSession({ pool, tableName: 'session', createTableIfMissing: false }),
  secret: SESSION_SECRET, resave: false, saveUninitialized: false, name: '__wn_sid',
  cookie: { httpOnly: true, secure: IS_PROD, sameSite: 'strict', maxAge: 7*24*60*60*1000 },
}));

const loginLimiter = rateLimit({ windowMs: 15*60*1000, max: 10, message: { error: 'Слишком много попыток. Подождите 15 минут.' }, keyGenerator: r => hashIP(r.ip) });
const apiLimiter   = rateLimit({ windowMs: 60*1000, max: 200, message: { error: 'Слишком много запросов.' }, keyGenerator: r => hashIP(r.ip) });
app.use('/api/', apiLimiter);

function hashIP(ip) { return crypto.createHash('sha256').update((ip||'')+SESSION_SECRET).digest('hex').slice(0,16); }

// ═══════════════════════════════════════════════════════════════════════
// ЖУРНАЛ РЕДАКТИРОВАНИЯ ПОЛЕЙ (кто/что/когда изменил, значение до и после)
// ═══════════════════════════════════════════════════════════════════════
const EDIT_LOG_FIELD_LABELS = {
  news:{title:'Заголовок',category:'Категория',excerpt:'Анонс',blocks:'Содержимое статьи',img:'Изображение',bg_img:'Фон',align:'Выравнивание',title_color:'Цвет заголовка',text_color:'Цвет текста',author_name:'Автор',created_at:'Дата публикации'},
  service:{name:'Название',items:'Позиции'},
  team_cat:{name:'Название категории',layout:'Расположение'},
  team_member:{cat_id:'Категория',name:'Имя',role:'Должность',photo:'Фото',role_font:'Шрифт должности'},
  employee:{name:'Имя',static_id:'Static ID',active:'Активен'},
  contract_slot:{price:'Цена контракта',text:'Текст',accepted_id:'Принял',declined_id:'Откинул',payout:'К выплате',status:'Статус',transfer_time:'Время переноса'},
  bonus:{amount:'Сумма',comment:'Комментарий',paid:'Выплачено'},
  user_role:{role:'Роль'},
};
function truncForLog(v,len=300){
  if(v===null||v===undefined)return '';
  let s=typeof v==='string'?v:JSON.stringify(v);
  if(s.length>len)s=s.slice(0,len)+'…';
  return s;
}
const boolLbl=v=>v===true?'Да':v===false?'Нет':'';
// Сравнивает before (строка из БД ДО изменения) с after (строка из БД
// ПОСЛЕ изменения) по карте fieldLabels и, если есть хоть одно отличие,
// пишет одну запись в edit_logs со списком изменений. Раз сравниваются
// два реальных снимка из БД, а не то, что пришло в запросе — поля с
// COALESCE-фолбэками (например «оставить как было, если не передано»)
// не дадут ложных срабатываний. Молча ничего не делает при ошибке —
// журнал не должен мешать сохранению.
async function logFieldEdit(req,entity,entityId,entityLabel,before,after,fieldLabels){
  try{
    if(!fieldLabels||!before||!after)return;
    const changes=[];
    for(const key of Object.keys(fieldLabels)){
      const bs=truncForLog(before[key]);
      const as=truncForLog(after[key]);
      if(bs!==as)changes.push({field:fieldLabels[key],before:bs,after:as});
    }
    if(!changes.length)return;
    await query(
      `INSERT INTO edit_logs (id,user_id,user_name,entity,entity_id,entity_label,changes) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [uuid(),req.user?.id||null,req.user?.name||'Система',entity,(entityId||'').toString(),(entityLabel||'').toString().slice(0,200),JSON.stringify(changes)]
    );
    await query(`DELETE FROM edit_logs WHERE id NOT IN (SELECT id FROM edit_logs ORDER BY created_at DESC LIMIT 2000)`);
  }catch(e){ console.error('logFieldEdit error:',e.message); }
}
async function empNameMap(){
  const r=await query('SELECT id,name FROM employees');
  const m={}; r.rows.forEach(e=>{m[e.id]=e.name;}); return m;
}
function maskEmail(e) {
  if(!e)return'—'; const [l,d]=e.split('@'); if(!d)return e.slice(0,2)+'***';
  const ml=l.length>2?l.slice(0,2)+'*'.repeat(Math.min(l.length-2,4)):l;
  const p=d.split('.'); return ml+'@'+p[0].slice(0,2)+'***.'+p.slice(1).join('.');
}
function safeUser(u) { if(!u)return null; const {pwd_hash,...s}=u; return s; }
function parseJSON(s,d=[]) { try{return JSON.parse(s);}catch{return d;} }

// ─── Недельная статистика: понедельник—воскресенье, UTC-даты без времени ───
function mondayOf(d){
  const dt=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate()));
  const day=dt.getUTCDay(); const diff=(day===0?-6:1-day);
  dt.setUTCDate(dt.getUTCDate()+diff); return dt;
}
function fmtDate(d){ return d.toISOString().slice(0,10); }
function weekRange(offset=0){
  const thisMonday=mondayOf(new Date());
  const start=new Date(thisMonday); start.setUTCDate(start.getUTCDate()-offset*7);
  const end=new Date(start); end.setUTCDate(end.getUTCDate()+6);
  return { start:fmtDate(start), end:fmtDate(end) };
}
function weekRangeForDate(dateStr){
  const mon=mondayOf(new Date(dateStr));
  const end=new Date(mon); end.setUTCDate(end.getUTCDate()+6);
  return { start:fmtDate(mon), end:fmtDate(end) };
}

// ─── Расписание слотов таблицы контрактов (по умолчанию 13:00 → 03:00 след. дня, шаг 10 мин) ───
async function getSchedule(){
  try{
    const r=await query(`SELECT value FROM site_settings WHERE key='contractSchedule'`);
    if(r.rows.length){ const v=JSON.parse(r.rows[0].value); if(v&&v.start&&v.end) return { start:v.start, end:v.end, intervalMin:Number(v.intervalMin)||10 }; }
  }catch{}
  return { start:'13:00', end:'03:00', intervalMin:10 };
}
function genTimeSlots(start,end,intervalMin){
  const toMin=t=>{ const [h,m]=t.split(':').map(Number); return h*60+m; };
  let s=toMin(start), e=toMin(end); if(e<=s) e+=24*60;
  const out=[];
  for(let t=s;t<e;t+=intervalMin){
    const hh=String(Math.floor((t%1440)/60)).padStart(2,'0');
    const mm=String(t%60).padStart(2,'0');
    out.push(`${hh}:${mm}`);
  }
  return out;
}

async function requireAuth(req,res,next){
  if(!req.session?.userId) return res.status(401).json({error:'Требуется авторизация'});
  const r=await query('SELECT * FROM users WHERE id=$1',[req.session.userId]);
  if(!r.rows.length){req.session.destroy(()=>{});return res.status(401).json({error:'Сессия недействительна'});}
  req.user=r.rows[0];next();
}
async function requireEditor(req,res,next){ await requireAuth(req,res,()=>{ if(!['editor','admin','leader'].includes(req.user.role))return res.status(403).json({error:'Нет прав'});next();}); }
async function requireAdmin(req,res,next){  await requireAuth(req,res,()=>{ if(req.user.role!=='admin')return res.status(403).json({error:'Только для администратора'});next();}); }
async function requireAdvertising(req,res,next){ await requireAuth(req,res,()=>{ if(!['advertising','curator_ad','editor','admin','leader'].includes(req.user.role))return res.status(403).json({error:'Нет доступа'});next();}); }
async function requireStaffMgmt(req,res,next){ await requireAuth(req,res,()=>{ if(!['curator_ad','editor','admin','leader'].includes(req.user.role))return res.status(403).json({error:'Нет доступа'});next();}); }

// Файл принимаем в память (buffer), а не сразу на диск: так его можно
// отправить в Cloudinary. Если Cloudinary не настроен — пишем этот же
// buffer на диск сами (см. роут /api/upload ниже).
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25*1024*1024 },
  fileFilter: (req,f,cb) => { if(!f.mimetype.startsWith('image/'))return cb(new Error('Только изображения')); cb(null,true); }
});
app.use('/uploads', express.static(UPLOADS_DIR));

// ─── GOOGLE APPS SCRIPT: поиск свободных слотов для объявлений ──────────────
// Вся логика поиска слотов живёт в скрипте ВНУТРИ самой Google Таблицы
// (Расширения → Apps Script), развёрнутом как веб-приложение.
// Наш сервер — просто прокси: получает запрос от сайта, пересылает
// в Apps Script, возвращает ответ. Настраивается ОДНОЙ переменной:
//   GOOGLE_APPS_SCRIPT_URL — ссылка вида https://script.google.com/macros/s/.../exec
// Полный код скрипта и инструкция — см. google-apps-script.gs и README.md

app.post('/api/booking/search', requireAdvertising, async (req, res) => {
  try {
    const scriptUrl = process.env.GOOGLE_APPS_SCRIPT_URL;
    if (!scriptUrl) {
      return res.status(503).json({ error: 'Google Таблица не подключена. Обратитесь к администратору сайта — нужно указать переменную GOOGLE_APPS_SCRIPT_URL.' });
    }

    let { color, days, adsPerDay } = req.body;
    color = color === 'red' ? 'red' : 'green';
    days = Math.max(2, Math.min(7, parseInt(days, 10) || 2));
    adsPerDay = Math.max(2, Math.min(10, parseInt(adsPerDay, 10) || 2));

    const url = `${scriptUrl}?color=${encodeURIComponent(color)}&days=${days}&adsPerDay=${adsPerDay}`;
    const resp = await fetch(url, { redirect: 'follow' });
    const rawText = await resp.text();

    // Apps Script при неверных настройках доступа ("Who has access")
    // может вернуть HTML-страницу входа Google вместо JSON. Ловим это явно,
    // а не даём упасть в невнятный SyntaxError.
    let data;
    try {
      data = JSON.parse(rawText);
    } catch {
      const looksLikeGoogleLogin = /accounts\.google\.com|ServiceLogin|<html/i.test(rawText);
      const hint = looksLikeGoogleLogin
        ? 'Похоже, Google вернул страницу входа вместо данных. Проверь в настройках развёртывания Apps Script: "Who has access" / "У кого есть доступ" должно быть "Anyone" / "Все", а не "Only myself".'
        : 'Ответ Apps Script не является JSON.';
      console.error('Ads search: non-JSON response from Apps Script. Status:', resp.status, 'Snippet:', rawText.slice(0, 300));
      return res.status(502).json({ error: `Google Apps Script вернул некорректный ответ (код ${resp.status}). ${hint}` });
    }

    if (!resp.ok) {
      return res.status(502).json({ error: data.error || `Google Apps Script вернул ошибку (код ${resp.status}).` });
    }
    if (data.error) return res.status(502).json({ error: data.error });

    res.json(data);
  } catch (e) {
    console.error('Ads search error:', e.message);
    res.status(500).json({ error: 'Не удалось связаться с Google Apps Script: ' + e.message });
  }
});

// AUTH
app.post('/api/auth/login', loginLimiter, async (req,res) => {
  try {
    const {email,password}=req.body;
    if(!email||!password)return res.status(400).json({error:'Заполните все поля'});
    const ipHash=hashIP(req.ip);
    const att=await query('SELECT * FROM login_attempts WHERE ip_hash=$1',[ipHash]);
    const attempt=att.rows[0];
    if(attempt?.locked_until&&new Date(attempt.locked_until)>new Date()){
      const secs=Math.ceil((new Date(attempt.locked_until)-Date.now())/1000);
      return res.status(429).json({error:`Заблокировано. Подождите ${secs} сек.`});
    }
    const r=await query('SELECT * FROM users WHERE email=$1',[email.trim().toLowerCase()]);
    const user=r.rows[0];
    const hashToCheck=user?.pwd_hash||'$2b$12$invalidhashXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';
    const match=await bcrypt.compare(password,hashToCheck);
    if(!user||!match){
      const nc=(attempt?.count||0)+1;
      const lock=nc>=5?new Date(Date.now()+60000).toISOString():null;
      await query('INSERT INTO login_attempts (ip_hash,count,locked_until) VALUES ($1,$2,$3) ON CONFLICT (ip_hash) DO UPDATE SET count=$2,locked_until=$3',[ipHash,nc,lock]);
      return res.status(401).json({error:'Неверная почта или пароль'});
    }
    await query('DELETE FROM login_attempts WHERE ip_hash=$1',[ipHash]);
    await query('UPDATE users SET last_login=NOW() WHERE id=$1',[user.id]);
    req.session.regenerate(err=>{
      if(err)return res.status(500).json({error:'Ошибка сессии'});
      req.session.userId=user.id;
      res.json({user:safeUser(user)});
    });
  } catch(e){console.error(e.message);res.status(500).json({error:'Ошибка сервера'});}
});

app.post('/api/auth/register', loginLimiter, async (req,res) => {
  try {
    const {name,email,password}=req.body;
    if(!name||!email||!password)return res.status(400).json({error:'Заполните все поля'});
    if(password.length<6)return res.status(400).json({error:'Пароль минимум 6 символов'});
    if(!/\S+@\S+\.\S+/.test(email))return res.status(400).json({error:'Некорректный email'});
    const ex=await query('SELECT id FROM users WHERE email=$1',[email.trim().toLowerCase()]);
    if(ex.rows.length)return res.status(409).json({error:'Email уже используется'});
    const hash=await bcrypt.hash(password,BCRYPT_ROUNDS);
    const id=uuid();
    await query('INSERT INTO users (id,name,email,pwd_hash,role) VALUES ($1,$2,$3,$4,$5)',[id,name.trim(),email.trim().toLowerCase(),hash,'guest']);
    const r=await query('SELECT * FROM users WHERE id=$1',[id]);
    req.session.regenerate(err=>{
      if(err)return res.status(500).json({error:'Ошибка сессии'});
      req.session.userId=id;res.json({user:safeUser(r.rows[0])});
    });
  } catch(e){console.error(e.message);res.status(500).json({error:'Ошибка сервера'});}
});

app.post('/api/auth/logout',(req,res)=>{ req.session.destroy(()=>res.json({ok:true})); });
app.get('/api/auth/me', async (req,res)=>{ if(!req.session?.userId)return res.json({user:null}); try{const r=await query('SELECT * FROM users WHERE id=$1',[req.session.userId]);res.json({user:safeUser(r.rows[0])||null});}catch{res.json({user:null});} });

// USERS
// Просмотр списка пользователей — доступен Curator AD и выше (Редактор, Администратор).
app.get('/api/users',requireStaffMgmt,async(req,res)=>{ const r=await query('SELECT id,name,email,role,created_at,last_login FROM users ORDER BY created_at');res.json(r.rows.map(u=>({...u,email:maskEmail(u.email)})));});

// Изменение роли — иерархия:
//  • Администратор — может назначить любую роль любому пользователю.
//  • Лидер (leader) — доступ как у Администратора почти везде (см.
//    requireEditor/requireAdvertising/requireStaffMgmt выше), включая
//    управление ролями; НЕ видит статистику посещений и журнал
//    редактирования (эти два роута остались на requireAdmin).
//  • Curator AD — эксклюзивное право выдавать/снимать ИМЕННО роль Advertising Department,
//    и только пользователям, которые сейчас Guest либо уже Advertising (не может трогать более высокие роли).
//  • Редактор — доступ к списку только на просмотр, менять роли не может (это зона ответственности Curator AD).
app.put('/api/users/:id/role',requireStaffMgmt,async(req,res)=>{
  const{role}=req.body;
  const ROLES=['guest','advertising','curator_ad','editor','leader','admin'];
  if(!ROLES.includes(role))return res.status(400).json({error:'Неверная роль'});
  if(req.params.id===req.user.id)return res.status(400).json({error:'Нельзя изменить свою роль'});
  if(req.user.role==='editor'){
    return res.status(403).json({error:'Изменение ролей доступно только Curator AD, Лидеру или Администратору'});
  }
  if(req.user.role==='curator_ad'){
    if(!['guest','advertising'].includes(role))return res.status(403).json({error:'Curator AD может назначать только роль Advertising Department'});
    const targetR=await query('SELECT role FROM users WHERE id=$1',[req.params.id]);
    if(!targetR.rows.length)return res.status(404).json({error:'Пользователь не найден'});
    if(!['guest','advertising'].includes(targetR.rows[0].role))return res.status(403).json({error:'Недостаточно прав для изменения этой роли'});
  }
  const beforeR=await query('SELECT name,role FROM users WHERE id=$1',[req.params.id]);
  if(!beforeR.rows.length)return res.status(404).json({error:'Пользователь не найден'});
  await query('UPDATE users SET role=$1 WHERE id=$2',[role,req.params.id]);
  await logFieldEdit(req,'user_role',req.params.id,beforeR.rows[0].name,beforeR.rows[0],{role},EDIT_LOG_FIELD_LABELS.user_role);
  res.json({ok:true});
});

// ═══════════════════════════════════════════════════════════════════════
// СОТРУДНИКИ (роster отдела рекламы: имя персонажа + StaticID)
// Используется в выпадающих списках «Принял/Откинул» таблицы контрактов
// и в недельной статистике. Просмотр — Advertising Department и выше;
// добавление/редактирование/удаление — только Curator AD и выше.
// ═══════════════════════════════════════════════════════════════════════
app.get('/api/employees',requireAdvertising,async(req,res)=>{
  try{ const r=await query('SELECT * FROM employees ORDER BY sort_order,name'); res.json(r.rows); }
  catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/employees',requireStaffMgmt,async(req,res)=>{
  try{
    const{name,static_id}=req.body;
    if(!name?.trim())return res.status(400).json({error:'Укажите имя сотрудника'});
    const id=uuid();
    const maxR=await query('SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM employees');
    await query('INSERT INTO employees (id,name,static_id,sort_order) VALUES ($1,$2,$3,$4)',[id,name.trim(),(static_id||'').toString().trim(),maxR.rows[0].n]);
    res.json({id,name:name.trim(),static_id:(static_id||'').toString().trim(),active:true});
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/employees/:id',requireStaffMgmt,async(req,res)=>{
  try{
    const{name,static_id,active}=req.body;
    const before=await query('SELECT * FROM employees WHERE id=$1',[req.params.id]);
    await query('UPDATE employees SET name=COALESCE($1,name), static_id=COALESCE($2,static_id), active=COALESCE($3,active) WHERE id=$4',
      [name===undefined?null:name.trim(), static_id===undefined?null:(static_id||'').toString().trim(), active===undefined?null:active, req.params.id]);
    if(before.rows.length){
      const after=await query('SELECT * FROM employees WHERE id=$1',[req.params.id]);
      const b={...before.rows[0], active:boolLbl(before.rows[0].active)};
      const a={...after.rows[0], active:boolLbl(after.rows[0].active)};
      await logFieldEdit(req,'employee',req.params.id,after.rows[0].name,b,a,EDIT_LOG_FIELD_LABELS.employee);
    }
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/employees/:id',requireStaffMgmt,async(req,res)=>{
  try{ await query('DELETE FROM employees WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){res.status(500).json({error:e.message});}
});

// ═══════════════════════════════════════════════════════════════════════
// ТАБЛИЦА КОНТРАКТОВ (интерактивное расписание объявлений на день)
// Просмотр — доступен Advertising Department и выше (Curator AD, Редактор,
// Администратор). Редактирование большинства полей — тоже, НО для роли
// Advertising Department поля «Цена контракта», «Текст», «Принял» и
// «К выплате» доступны только на просмотр (см. проверку внутри PUT ниже) —
// им можно менять статус, «Откинул» и время переноса.
// ═══════════════════════════════════════════════════════════════════════
app.get('/api/contracts',requireAdvertising,async(req,res)=>{
  try{
    let{color,date}=req.query;
    color=color==='red'?'red':'green';
    if(!date||isNaN(Date.parse(date)))date=new Date().toISOString().slice(0,10);
    const sched=await getSchedule();
    const slots=genTimeSlots(sched.start,sched.end,sched.intervalMin||10);
    for(const t of slots){
      await query('INSERT INTO contract_slots (id,color,slot_date,slot_time) VALUES ($1,$2,$3,$4) ON CONFLICT (color,slot_date,slot_time) DO NOTHING',[uuid(),color,date,t]);
    }
    const r=await query(`
      SELECT cs.*, ea.name AS accepted_name, ed.name AS declined_name
      FROM contract_slots cs
      LEFT JOIN employees ea ON ea.id=cs.accepted_id
      LEFT JOIN employees ed ON ed.id=cs.declined_id
      WHERE cs.color=$1 AND cs.slot_date=$2`,[color,date]);
    const order={}; slots.forEach((t,i)=>order[t]=i);
    r.rows.sort((a,b)=>(order[a.slot_time]??0)-(order[b.slot_time]??0));
    res.json({ color, date, schedule:sched, slots:r.rows });
  }catch(e){res.status(500).json({error:e.message});}
});

app.put('/api/contracts/:id',requireAdvertising,async(req,res)=>{
  try{
    const{status,price,text,accepted_id,declined_id,payout,transfer_time}=req.body;
    // Advertising Department видит контракты, но не имеет права менять цену,
    // текст, принявшего сотрудника и сумму к выплате — это зона ответственности
    // Curator AD и выше. Им доступны только статус, «Откинул» и время переноса.
    if(req.user.role==='advertising'){
      const forbidden=['price','text','accepted_id','payout'].filter(f=>req.body[f]!==undefined);
      if(forbidden.length)return res.status(403).json({error:'Advertising Dept. может менять только статус, «Откинул» и время переноса'});
    }
    const cur=await query('SELECT * FROM contract_slots WHERE id=$1',[req.params.id]);
    if(!cur.rows.length)return res.status(404).json({error:'Слот не найден'});
    const c=cur.rows[0];
    await query(`UPDATE contract_slots SET
        status=COALESCE($1,status), price=COALESCE($2,price), text=COALESCE($3,text),
        accepted_id=$4, declined_id=$5, payout=COALESCE($6,payout), transfer_time=COALESCE($7,transfer_time),
        updated_at=NOW()
      WHERE id=$8`,
      [ status===undefined?null:status,
        price===undefined?null:price,
        text===undefined?null:text,
        accepted_id===undefined?c.accepted_id:(accepted_id||null),
        declined_id===undefined?c.declined_id:(declined_id||null),
        payout===undefined?null:payout,
        transfer_time===undefined?null:transfer_time,
        req.params.id ]);
    const r=await query(`
      SELECT cs.*, ea.name AS accepted_name, ed.name AS declined_name
      FROM contract_slots cs
      LEFT JOIN employees ea ON ea.id=cs.accepted_id
      LEFT JOIN employees ed ON ed.id=cs.declined_id
      WHERE cs.id=$1`,[req.params.id]);
    // Для лога подменяем ID сотрудников на имена (сырой UUID в журнале
    // редактирования бесполезен) — «после» уже есть из JOIN выше, «до»
    // разрешаем через employees.
    const empMap=await empNameMap();
    const beforeResolved={...c, accepted_id:c.accepted_id?(empMap[c.accepted_id]||'—'):'—', declined_id:c.declined_id?(empMap[c.declined_id]||'—'):'—', status:boolLbl(c.status)};
    const afterRow=r.rows[0];
    const afterResolved={...afterRow, accepted_id:afterRow.accepted_name||'—', declined_id:afterRow.declined_name||'—', status:boolLbl(afterRow.status)};
    const dateStr=c.slot_date instanceof Date?c.slot_date.toISOString().slice(0,10):String(c.slot_date).slice(0,10);
    const label=`Контракт ${c.slot_time} (${c.color==='green'?'зел.':'красн.'}) ${dateStr}`;
    await logFieldEdit(req,'contract_slot',req.params.id,label,beforeResolved,afterResolved,EDIT_LOG_FIELD_LABELS.contract_slot);
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

// ═══════════════════════════════════════════════════════════════════════
// ДОБАВИТЬ КОНТРАКТ (массовое заполнение слотов из вкладки «Добавить контракт»)
// Принимает: color, times (список ЧЧ:ММ — если объявлений в день несколько),
// dates (список дат — всегда начиная со следующего дня, считает фронтенд),
// text, accepted_id, discount. Проверяет, что КАЖДАЯ пара время×дата свободна
// (текст пуст и сотрудник не назначен); если занято хоть одно — ничего не
// меняет и возвращает список занятых слотов. Если всё свободно — одним
// действием проставляет текст/принявшего сотрудника; цена контракта (price)
// это полная сумма заказа за весь контракт, а payout — выплата за 1 объявление,
// считаются по той же формуле, что в Калькуляторе.
// ═══════════════════════════════════════════════════════════════════════
app.post('/api/contracts/bulk', requireStaffMgmt, async (req, res) => {
  try {
    let { color, times, dates, text, accepted_id, discount } = req.body;
    color = color === 'red' ? 'red' : 'green';
    text = (text || '').toString().trim();
    if (!text) return res.status(400).json({ error: 'Укажите текст объявления' });
    if (!accepted_id) return res.status(400).json({ error: 'Выберите сотрудника, принявшего контракт' });

    const emp = await query('SELECT id FROM employees WHERE id=$1', [accepted_id]);
    if (!emp.rows.length) return res.status(400).json({ error: 'Сотрудник не найден' });

    const timeRe = /^([01]\d|2[0-3]):[0-5]\d$/;
    if (!Array.isArray(times) || !times.length) return res.status(400).json({ error: 'Укажите хотя бы одно время' });
    times = [...new Set(times)];
    if (times.some(t => !timeRe.test(t))) return res.status(400).json({ error: 'Некорректный формат времени' });

    if (!Array.isArray(dates) || !dates.length) return res.status(400).json({ error: 'Укажите срок контракта' });
    dates = [...new Set(dates)];
    if (dates.some(d => isNaN(Date.parse(d)))) return res.status(400).json({ error: 'Некорректная дата' });

    // Контракт обязательно заполняется начиная не раньше чем с завтрашнего дня
    const tomorrow = new Date(); tomorrow.setUTCHours(0, 0, 0, 0); tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
    const tomorrowStr = tomorrow.toISOString().slice(0, 10);
    if (dates.some(d => d < tomorrowStr)) {
      return res.status(400).json({ error: 'Контракт можно заполнять только начиная со следующего дня' });
    }

    discount = parseFloat(discount); if (isNaN(discount) || discount < 0) discount = 0; if (discount > 100) discount = 100;

    // Времена должны входить в действующее расписание слотов таблицы контрактов
    const sched = await getSchedule();
    const validTimes = new Set(genTimeSlots(sched.start, sched.end, sched.intervalMin || 10));
    if (times.some(t => !validTimes.has(t))) {
      return res.status(400).json({ error: 'Одно из указанных времён не входит в расписание слотов' });
    }

    const pairs = [];
    for (const d of dates) for (const t of times) pairs.push({ d, t });

    // Гарантируем существование строк слотов на каждую нужную пару (дата,время)
    for (const { d, t } of pairs) {
      await query('INSERT INTO contract_slots (id,color,slot_date,slot_time) VALUES ($1,$2,$3,$4) ON CONFLICT (color,slot_date,slot_time) DO NOTHING', [uuid(), color, d, t]);
    }

    // Проверяем свободны ли нужные слоты (слот свободен, если текст пуст и сотрудник не назначен)
    const existing = await query(
      `SELECT to_char(slot_date,'YYYY-MM-DD') AS d, slot_time AS t, text, accepted_id
       FROM contract_slots WHERE color=$1 AND slot_date = ANY($2::date[]) AND slot_time = ANY($3::text[])`,
      [color, dates, times]
    );
    const map = new Map();
    existing.rows.forEach(r => map.set(`${r.d}_${r.t}`, r));

    const busy = [];
    for (const { d, t } of pairs) {
      const row = map.get(`${d}_${t}`);
      const free = row && (!row.text || !row.text.trim()) && !row.accepted_id;
      if (!free) busy.push({ date: d, time: t });
    }
    if (busy.length) return res.status(409).json({ error: 'Некоторые слоты уже заняты', busy });

    // Расчёт как в Калькуляторе: символ × 300 (зелёные) / × 150 (красные); казна 90% / сотрудник 10%
    const rate = color === 'red' ? 150 : 300;
    const chars = text.length;
    const totalAds = times.length * dates.length;
    const baseSum = totalAds * chars * rate;
    const orderSum = baseSum * (1 - discount / 100);
    const treasury = orderSum * 0.9;
    const toEmployee = orderSum * 0.1;
    const perAd = totalAds > 0 ? toEmployee / totalAds : 0;

    // В саму таблицу «Контракты» текст сохраняется с префиксом команды /wnews —
    // так его можно сразу скопировать и вставить в игровой чат. Цена и выплата
    // при этом считаются по исходному тексту БЕЗ префикса (см. chars/orderSum выше),
    // чтобы префикс не влиял на стоимость объявления.
    const wnewsText = `/wnews ${text}`;
    for (const { d, t } of pairs) {
      await query(
        `UPDATE contract_slots SET text=$1, accepted_id=$2, price=$3, payout=$4, updated_at=NOW() WHERE color=$5 AND slot_date=$6 AND slot_time=$7`,
        [wnewsText, accepted_id, orderSum, perAd, color, d, t]
      );
    }

    res.json({
      ok: true, filled: pairs.length, color, dates, times,
      calc: { chars, totalAds, rate, discount, baseSum, orderSum, treasury, toEmployee, perAd }
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════
// НЕДЕЛЬНАЯ СТАТИСТИКА
// offset=0 — текущая неделя, offset=1 — прошлая. Параметр date — точечный
// просмотр произвольной недели из глубокого архива (данные не удаляются,
// просто не показываются вкладками по умолчанию).
// Просмотр доступен Advertising Department и выше — управление премиями
// (создание/редактирование/удаление) по-прежнему только Curator AD и выше.
// ═══════════════════════════════════════════════════════════════════════
app.get('/api/stats/week',requireAdvertising,async(req,res)=>{
  try{
    let offset=parseInt(req.query.offset,10); if(isNaN(offset)||offset<0)offset=0;
    const range=(req.query.date&&!isNaN(Date.parse(req.query.date))) ? weekRangeForDate(req.query.date) : weekRange(offset);
    const emps=await query('SELECT * FROM employees ORDER BY sort_order,name');
    const slots=await query('SELECT * FROM contract_slots WHERE slot_date BETWEEN $1 AND $2',[range.start,range.end]);
    const stats={};
    emps.rows.forEach(e=>{ stats[e.id]={ id:e.id, name:e.name, static_id:e.static_id, acceptedGreen:0, sentGreen:0, acceptedRed:0, sentRed:0, payout:0, declinedCount:0 }; });
    slots.rows.forEach(s=>{
      if(s.accepted_id && stats[s.accepted_id]){
        const st=stats[s.accepted_id];
        if(s.color==='green')st.acceptedGreen++; else st.acceptedRed++;
      }
      // «Отправлено», «К выплате» и сам факт участия сотрудника в «Откинул»
      // начисляются ТОЛЬКО когда у слота отмечена галочка «Статус» (ад
      // фактически отправлен) — просто вписанное имя в «Откинул» само по
      // себе в статистику не идёт, иначе деньги/счётчик засчитывались бы
      // ещё до реальной отправки объявления.
      if(s.declined_id && stats[s.declined_id] && s.status){
        const st=stats[s.declined_id];
        st.declinedCount++;
        st.payout+=Number(s.payout)||0;
        if(s.color==='green')st.sentGreen++; else st.sentRed++;
      }
    });
    const bonuses=await query(`SELECT b.*, e.name AS emp_name, e.static_id FROM bonuses b LEFT JOIN employees e ON e.id=b.employee_id WHERE b.week_start=$1 ORDER BY b.created_at`,[range.start]);
    res.json({ range, employees:Object.values(stats), bonuses:bonuses.rows });
  }catch(e){res.status(500).json({error:e.message});}
});

// ─── Премирование ───
app.post('/api/bonuses',requireStaffMgmt,async(req,res)=>{
  try{
    const{employee_id,week_start,amount,comment}=req.body;
    if(!employee_id||!week_start||isNaN(Date.parse(week_start)))return res.status(400).json({error:'Укажите сотрудника и неделю'});
    const id=uuid();
    await query('INSERT INTO bonuses (id,employee_id,week_start,amount,comment) VALUES ($1,$2,$3,$4,$5)',[id,employee_id,week_start,Number(amount)||0,(comment||'').toString().slice(0,300)]);
    const r=await query('SELECT b.*, e.name AS emp_name, e.static_id FROM bonuses b LEFT JOIN employees e ON e.id=b.employee_id WHERE b.id=$1',[id]);
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/bonuses/:id',requireStaffMgmt,async(req,res)=>{
  try{
    const{amount,comment,paid}=req.body;
    const before=await query('SELECT b.*, e.name AS emp_name FROM bonuses b LEFT JOIN employees e ON e.id=b.employee_id WHERE b.id=$1',[req.params.id]);
    await query('UPDATE bonuses SET amount=COALESCE($1,amount), comment=COALESCE($2,comment), paid=COALESCE($3,paid) WHERE id=$4',
      [amount===undefined?null:Number(amount), comment===undefined?null:comment.toString().slice(0,300), paid===undefined?null:paid, req.params.id]);
    if(before.rows.length){
      const after=await query('SELECT b.*, e.name AS emp_name FROM bonuses b LEFT JOIN employees e ON e.id=b.employee_id WHERE b.id=$1',[req.params.id]);
      const label=`Премия: ${before.rows[0].emp_name||'—'}`;
      const b={...before.rows[0], paid:boolLbl(before.rows[0].paid)};
      const a={...after.rows[0], paid:boolLbl(after.rows[0].paid)};
      await logFieldEdit(req,'bonus',req.params.id,label,b,a,EDIT_LOG_FIELD_LABELS.bonus);
    }
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/bonuses/:id',requireStaffMgmt,async(req,res)=>{
  try{ await query('DELETE FROM bonuses WHERE id=$1',[req.params.id]); res.json({ok:true}); }
  catch(e){res.status(500).json({error:e.message});}
});

// UPLOAD
// Единая точка загрузки картинок для всего сайта: фоны страниц,
// аватарки участников состава ("Состав"), изображения новостей и т.д.
// Все они используют этот один роут — поэтому подключение Cloudinary
// здесь автоматически чинит проблему с потерей файлов при редеплое
// сразу везде, включая аватарки.
app.post('/api/upload',requireEditor,upload.single('image'),async(req,res)=>{
  if(!req.file)return res.status(400).json({error:'Файл не загружен'});
  try{
    if(CLOUDINARY_ENABLED){
      const buf=await shrinkImageIfNeeded(req.file.buffer,req.file.mimetype);
      const result=await uploadBufferToCloudinary(buf);
      return res.json({url:result.secure_url});
    }
    const ext=path.extname(req.file.originalname||'').toLowerCase().replace(/[^.a-z0-9]/g,'')||'.jpg';
    const filename=uuid()+ext;
    fs.writeFileSync(path.join(UPLOADS_DIR,filename),req.file.buffer);
    return res.json({url:`/uploads/${filename}`});
  }catch(e){
    console.error('Ошибка загрузки файла:',e.message);
    return res.status(500).json({error:'Не удалось загрузить файл: '+e.message});
  }
});

// NEWS
app.get('/api/news',async(req,res)=>{ try{const r=await query('SELECT * FROM news ORDER BY created_at DESC');res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}});
app.post('/api/news',requireEditor,async(req,res)=>{ try{const{title,category,excerpt,blocks,img,bg_img,align,title_color,text_color,created_at,author_name}=req.body;if(!title?.trim())return res.status(400).json({error:'Укажите заголовок'});const id=uuid();let dateVal=null;if(created_at){const d=new Date(created_at);if(!isNaN(d.getTime()))dateVal=d.toISOString();}const finalAuthor=(author_name&&author_name.trim())?author_name.trim().slice(0,100):req.user.name;await query('INSERT INTO news (id,title,category,excerpt,blocks,img,bg_img,align,title_color,text_color,author_id,author_name,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,COALESCE($13,NOW()))',[id,title.trim(),category||'',excerpt||'',blocks||'[]',img||'',bg_img||'',align||'left',title_color||'',text_color||'',req.user.id,finalAuthor,dateVal]);const r=await query('SELECT * FROM news WHERE id=$1',[id]);res.json(r.rows[0]);}catch(e){res.status(500).json({error:e.message});}});
app.put('/api/news/:id',requireEditor,async(req,res)=>{ try{const{title,category,excerpt,blocks,img,bg_img,align,title_color,text_color,created_at,author_name}=req.body;if(!title?.trim())return res.status(400).json({error:'Укажите заголовок'});let dateVal=null;if(created_at){const d=new Date(created_at);if(!isNaN(d.getTime()))dateVal=d.toISOString();}const authorVal=(author_name&&author_name.trim())?author_name.trim().slice(0,100):null;const before=await query('SELECT * FROM news WHERE id=$1',[req.params.id]);await query('UPDATE news SET title=$1,category=$2,excerpt=$3,blocks=$4,img=$5,bg_img=$6,align=$7,title_color=$8,text_color=$9,author_name=COALESCE($10,author_name),created_at=COALESCE($11,created_at),updated_at=NOW() WHERE id=$12',[title.trim(),category||'',excerpt||'',blocks||'[]',img||'',bg_img||'',align||'left',title_color||'',text_color||'',authorVal,dateVal,req.params.id]);if(before.rows.length){const after=await query('SELECT * FROM news WHERE id=$1',[req.params.id]);await logFieldEdit(req,'news',req.params.id,title.trim(),before.rows[0],after.rows[0],EDIT_LOG_FIELD_LABELS.news);}res.json({ok:true});}catch(e){res.status(500).json({error:e.message});}});
app.delete('/api/news/:id',requireEditor,async(req,res)=>{ await query('DELETE FROM news WHERE id=$1',[req.params.id]);res.json({ok:true});});

// SERVICES
app.get('/api/services',async(req,res)=>{ const r=await query('SELECT * FROM services ORDER BY sort_order');res.json(r.rows.map(s=>({...s,items:parseJSON(s.items)})));});
app.post('/api/services',requireEditor,async(req,res)=>{
  const{name,items}=req.body;if(!name?.trim())return res.status(400).json({error:'Укажите название'});
  // Защита от дублей при повторной/двойной отправке одной и той же формы
  // (например, двойной клик по кнопке «Сохранить» до того, как пришёл ответ сервера):
  // если точно такая же категория (имя + услуги) была создана в последние 10 секунд — не создаём вторую.
  const dup=await query(
    `SELECT id FROM services WHERE name=$1 AND items=$2 AND created_at > NOW() - INTERVAL '10 seconds' ORDER BY created_at DESC LIMIT 1`,
    [name.trim(),JSON.stringify(items||[])]
  ).catch(()=>({rows:[]}));
  if(dup.rows.length)return res.json({id:dup.rows[0].id,name,items:items||[]});
  const id=uuid();await query('INSERT INTO services (id,name,items) VALUES ($1,$2,$3)',[id,name.trim(),JSON.stringify(items||[])]);res.json({id,name,items:items||[]});
});
app.put('/api/services/:id',requireEditor,async(req,res)=>{ const{name,items}=req.body;const before=await query('SELECT * FROM services WHERE id=$1',[req.params.id]);await query('UPDATE services SET name=$1,items=$2 WHERE id=$3',[name,JSON.stringify(items||[]),req.params.id]);if(before.rows.length){const after=await query('SELECT * FROM services WHERE id=$1',[req.params.id]);await logFieldEdit(req,'service',req.params.id,name||before.rows[0].name,before.rows[0],after.rows[0],EDIT_LOG_FIELD_LABELS.service);}res.json({ok:true});});
app.delete('/api/services/:id',requireEditor,async(req,res)=>{ await query('DELETE FROM services WHERE id=$1',[req.params.id]);res.json({ok:true});});

// TEAM
app.get('/api/team',async(req,res)=>{ const c=await query('SELECT * FROM team_cats ORDER BY sort_order');const m=await query('SELECT * FROM team_members ORDER BY sort_order');res.json({cats:c.rows,members:m.rows});});

app.post('/api/team/cats',requireEditor,async(req,res)=>{
  const{name,layout}=req.body;if(!name?.trim())return res.status(400).json({error:'Укажите название'});
  const id=uuid();
  const maxR=await query('SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM team_cats');
  await query('INSERT INTO team_cats (id,name,layout,sort_order) VALUES ($1,$2,$3,$4)',[id,name.trim(),layout||'pyramid',maxR.rows[0].n]);
  res.json({id,name,layout:layout||'pyramid'});
});
app.put('/api/team/cats/:id',requireEditor,async(req,res)=>{ const{name,layout}=req.body;const before=await query('SELECT * FROM team_cats WHERE id=$1',[req.params.id]);await query('UPDATE team_cats SET name=$1,layout=$2 WHERE id=$3',[name,layout||'pyramid',req.params.id]);if(before.rows.length){const after=await query('SELECT * FROM team_cats WHERE id=$1',[req.params.id]);await logFieldEdit(req,'team_cat',req.params.id,name||before.rows[0].name,before.rows[0],after.rows[0],EDIT_LOG_FIELD_LABELS.team_cat);}res.json({ok:true});});
app.delete('/api/team/cats/:id',requireEditor,async(req,res)=>{ await query('DELETE FROM team_cats WHERE id=$1',[req.params.id]);res.json({ok:true});});

// Переместить категорию вверх/вниз (меняет местами sort_order с соседней категорией)
app.put('/api/team/cats/:id/move',requireEditor,async(req,res)=>{
  const{direction}=req.body;
  const cur=await query('SELECT * FROM team_cats WHERE id=$1',[req.params.id]);
  if(!cur.rows.length)return res.status(404).json({error:'Категория не найдена'});
  const curRow=cur.rows[0];
  const cmp=direction==='up'?'<':'>';
  const ord=direction==='up'?'DESC':'ASC';
  const neighborR=await query(`SELECT * FROM team_cats WHERE sort_order ${cmp} $1 ORDER BY sort_order ${ord} LIMIT 1`,[curRow.sort_order]);
  if(!neighborR.rows.length)return res.json({ok:true,moved:false});
  const neighbor=neighborR.rows[0];
  await query('UPDATE team_cats SET sort_order=$1 WHERE id=$2',[neighbor.sort_order,curRow.id]);
  await query('UPDATE team_cats SET sort_order=$1 WHERE id=$2',[curRow.sort_order,neighbor.id]);
  res.json({ok:true,moved:true});
});

app.post('/api/team/members',requireEditor,async(req,res)=>{
  const{cat_id,name,role,photo,role_font}=req.body;if(!name?.trim())return res.status(400).json({error:'Укажите имя'});
  const id=uuid();
  const maxR=await query('SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM team_members WHERE cat_id=$1',[cat_id]);
  await query('INSERT INTO team_members (id,cat_id,name,role,photo,role_font,sort_order) VALUES ($1,$2,$3,$4,$5,$6,$7)',
    [id,cat_id,name.trim(),role||'',photo||'',role_font||'',maxR.rows[0].n]);
  res.json({id,cat_id,name,role,photo,role_font});
});
app.put('/api/team/members/:id',requireEditor,async(req,res)=>{
  const{cat_id,name,role,photo,role_font}=req.body;
  const before=await query('SELECT * FROM team_members WHERE id=$1',[req.params.id]);
  await query('UPDATE team_members SET cat_id=$1,name=$2,role=$3,photo=$4,role_font=$5 WHERE id=$6',
    [cat_id,name,role||'',photo||'',role_font||'',req.params.id]);
  if(before.rows.length){
    const after=await query('SELECT * FROM team_members WHERE id=$1',[req.params.id]);
    await logFieldEdit(req,'team_member',req.params.id,name||before.rows[0].name,before.rows[0],after.rows[0],EDIT_LOG_FIELD_LABELS.team_member);
  }
  res.json({ok:true});
});
app.delete('/api/team/members/:id',requireEditor,async(req,res)=>{ await query('DELETE FROM team_members WHERE id=$1',[req.params.id]);res.json({ok:true});});

// Переместить участника вверх/вниз ВНУТРИ его категории
app.put('/api/team/members/:id/move',requireEditor,async(req,res)=>{
  const{direction}=req.body;
  const cur=await query('SELECT * FROM team_members WHERE id=$1',[req.params.id]);
  if(!cur.rows.length)return res.status(404).json({error:'Участник не найден'});
  const curRow=cur.rows[0];
  const cmp=direction==='up'?'<':'>';
  const ord=direction==='up'?'DESC':'ASC';
  const neighborR=await query(`SELECT * FROM team_members WHERE cat_id=$1 AND sort_order ${cmp} $2 ORDER BY sort_order ${ord} LIMIT 1`,[curRow.cat_id,curRow.sort_order]);
  if(!neighborR.rows.length)return res.json({ok:true,moved:false});
  const neighbor=neighborR.rows[0];
  await query('UPDATE team_members SET sort_order=$1 WHERE id=$2',[neighbor.sort_order,curRow.id]);
  await query('UPDATE team_members SET sort_order=$1 WHERE id=$2',[curRow.sort_order,neighbor.id]);
  res.json({ok:true,moved:true});
});


// SETTINGS
app.get('/api/settings',async(req,res)=>{ const r=await query('SELECT key,value FROM site_settings');const s={};r.rows.forEach(row=>{try{s[row.key]=JSON.parse(row.value);}catch{s[row.key]=row.value;}});res.json(s);});
app.put('/api/settings',requireEditor,async(req,res)=>{
  try{
    const changes=[];
    for(const[k,v]of Object.entries(req.body)){
      const oldR=await query('SELECT value FROM site_settings WHERE key=$1',[k]);
      const oldVal=oldR.rows.length?oldR.rows[0].value:'';
      const newVal=JSON.stringify(v);
      await query('INSERT INTO site_settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2',[k,newVal]);
      if(oldVal!==newVal)changes.push({field:k,before:truncForLog(oldVal),after:truncForLog(newVal)});
    }
    if(changes.length){
      await query(`INSERT INTO edit_logs (id,user_id,user_name,entity,entity_id,entity_label,changes) VALUES ($1,$2,$3,'settings','','Настройки сайта',$4)`,
        [uuid(),req.user?.id||null,req.user?.name||'Система',JSON.stringify(changes)]);
      await query(`DELETE FROM edit_logs WHERE id NOT IN (SELECT id FROM edit_logs ORDER BY created_at DESC LIMIT 2000)`);
    }
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

// VISITORS
app.post('/api/visitors',async(req,res)=>{ try{const{page}=req.body;let name='Гость';if(req.session?.userId){const r=await query('SELECT name FROM users WHERE id=$1',[req.session.userId]);name=r.rows[0]?.name||'Гость';}await query('INSERT INTO visitors (user_name,page,ip_hash) VALUES ($1,$2,$3)',[name,page||'?',hashIP(req.ip)]);await query('DELETE FROM visitors WHERE id NOT IN (SELECT id FROM visitors ORDER BY id DESC LIMIT 500)');res.json({ok:true});}catch{res.json({ok:true});}});
app.get('/api/visitors',requireAdmin,async(req,res)=>{ const r=await query('SELECT user_name,page,visited_at FROM visitors ORDER BY id DESC LIMIT 200');res.json(r.rows);});

// ═══════════════════════════════════════════════════════════════════════
// СТАТИСТИКА ПОСЕЩЕНИЙ САЙТА
// Клиент шлёт сюда ОДИН раз за визит (не на каждое переключение вкладок —
// см. logSiteVisit() во фронтенде, вызывается один раз при загрузке
// страницы), с visitor_id — случайным ID, который генерируется на клиенте
// и хранится в localStorage (переживает переключения вкладок и перезапуск
// браузера, привязан к конкретному устройству/браузеру). Благодаря
// UNIQUE(visitor_id,visit_date) + ON CONFLICT DO NOTHING один и тот же
// visitor_id за один день создаёт ровно одну запись, сколько бы раз
// человек ни заходил и ни обновлял страницу в этот день.
// ═══════════════════════════════════════════════════════════════════════
app.post('/api/site-visits',async(req,res)=>{
  try{
    const visitor_id=(req.body?.visitor_id||'').toString().trim().slice(0,128);
    if(!visitor_id)return res.json({ok:true});
    await query(
      `INSERT INTO site_visits (id,visitor_id,visit_date,ip_hash) VALUES ($1,$2,CURRENT_DATE,$3)
       ON CONFLICT (visitor_id,visit_date) DO NOTHING`,
      [uuid(),visitor_id,hashIP(req.ip)]
    );
    res.json({ok:true});
  }catch{res.json({ok:true});} // статистика не должна ломать работу сайта при сбое
});
app.get('/api/site-visits/stats',requireAdmin,async(req,res)=>{
  try{
    const [today,yesterday,last7,last30,allTime,daily]=await Promise.all([
      query(`SELECT COUNT(*)::int AS n FROM site_visits WHERE visit_date=CURRENT_DATE`),
      query(`SELECT COUNT(*)::int AS n FROM site_visits WHERE visit_date=CURRENT_DATE-1`),
      query(`SELECT COUNT(DISTINCT visitor_id)::int AS n FROM site_visits WHERE visit_date>=CURRENT_DATE-6`),
      query(`SELECT COUNT(DISTINCT visitor_id)::int AS n FROM site_visits WHERE visit_date>=CURRENT_DATE-29`),
      query(`SELECT COUNT(DISTINCT visitor_id)::int AS n FROM site_visits`),
      query(`SELECT to_char(visit_date,'YYYY-MM-DD') AS d, COUNT(*)::int AS n FROM site_visits
             WHERE visit_date>=CURRENT_DATE-13 GROUP BY visit_date ORDER BY visit_date`)
    ]);
    res.json({
      today:today.rows[0].n, yesterday:yesterday.rows[0].n,
      last7:last7.rows[0].n, last30:last30.rows[0].n, allTime:allTime.rows[0].n,
      daily:daily.rows.map(r=>({date:r.d,count:r.n}))
    });
  }catch(e){res.status(500).json({error:e.message});}
});

// Журнал редактирования полей — только Администратор (роль Leader сюда
// доступа не имеет, см. requireAdmin выше по аналогии с /api/visitors).
app.get('/api/edit-logs',requireAdmin,async(req,res)=>{
  try{
    const r=await query(`SELECT id,user_name,entity,entity_label,changes,created_at FROM edit_logs ORDER BY created_at DESC LIMIT 300`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// FRONTEND
app.use(express.static(path.join(__dirname,'public')));
app.get('*',(req,res)=>res.sendFile(path.join(__dirname,'public','index.html')));

app.use((err,req,res,next)=>{ if(err.code==='LIMIT_FILE_SIZE')return res.status(400).json({error:'Файл слишком большой (макс. 15MB)'});console.error(err.message);res.status(500).json({error:'Ошибка сервера'});});

initDB().then(()=>{ app.listen(PORT,'0.0.0.0',()=>console.log(`Weazel News: http://localhost:${PORT}`)); }).catch(e=>{ console.error('Ошибка запуска:',e.message);process.exit(1); });
