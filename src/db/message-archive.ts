/**
 * Generic cross-channel message archive.
 *
 * Writes inbound chat messages from every channel to store/messages.db in a
 * new `messages_v2` table alongside the v1 `messages` table (WhatsApp-only,
 * untouched for backward compat).
 *
 * On first open, migrates existing v1 `messages` rows into `messages_v2` with
 * channel_type='whatsapp' so historical data is immediately queryable.
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

import { STORE_DIR } from '../config.js';
import { log } from '../log.js';
import type { InboundEvent } from '../channels/adapter.js';

const DB_PATH = path.join(STORE_DIR, 'messages.db');

const SCHEMA = `
CREATE TABLE IF NOT EXISTS messages_v2 (
  id             TEXT PRIMARY KEY,
  channel_type   TEXT NOT NULL,
  platform_id    TEXT NOT NULL,
  thread_id      TEXT,
  timestamp      TEXT NOT NULL,
  sender_id      TEXT,
  sender_name    TEXT,
  content        TEXT NOT NULL,
  is_from_me     INTEGER NOT NULL DEFAULT 0,
  is_bot_message INTEGER NOT NULL DEFAULT 0,
  is_group       INTEGER NOT NULL DEFAULT 0,
  raw_content    TEXT
);
CREATE INDEX IF NOT EXISTS idx_mv2_platform_ts ON messages_v2(platform_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_mv2_ts          ON messages_v2(timestamp);
`;

// Migrate existing v1 WhatsApp rows on first open.
const MIGRATE_V1 = `
INSERT OR IGNORE INTO messages_v2
  (id, channel_type, platform_id, timestamp, sender_id, sender_name,
   content, is_from_me, is_bot_message, is_group)
SELECT id, 'whatsapp', chat_jid, timestamp, sender, sender_name,
       content, is_from_me, COALESCE(is_bot_message, 0), 0
FROM messages;
`;

let _db: Database.Database | null = null;
let _insert: Database.Statement | null = null;

function getArchiveDb(): Database.Database {
  if (_db) return _db;

  fs.mkdirSync(STORE_DIR, { recursive: true });
  _db = new Database(DB_PATH);
  _db.pragma('journal_mode = DELETE');
  _db.pragma('busy_timeout = 5000');
  _db.exec(SCHEMA);

  // One-time v1 migration: only runs if messages_v2 is empty and messages exists.
  try {
    const v2Count = (_db.prepare('SELECT COUNT(*) as n FROM messages_v2').get() as { n: number }).n;
    if (v2Count === 0) {
      const hasV1 = _db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages'").get();
      if (hasV1) {
        const { changes } = _db.prepare(MIGRATE_V1).run();
        if (changes > 0) {
          log.info('Migrated v1 WhatsApp messages to messages_v2', { count: changes });
        }
      }
    }
  } catch (err) {
    log.warn('messages_v2 migration skipped', { err });
  }

  _insert = _db.prepare(`
    INSERT OR IGNORE INTO messages_v2
      (id, channel_type, platform_id, thread_id, timestamp,
       sender_id, sender_name, content, is_from_me, is_bot_message, is_group, raw_content)
    VALUES
      ($id, $channel_type, $platform_id, $thread_id, $timestamp,
       $sender_id, $sender_name, $content, $is_from_me, $is_bot_message, $is_group, $raw_content)
  `);

  return _db;
}

function parseContent(raw: string): {
  text: string;
  senderId: string | null;
  senderName: string | null;
  isFromMe: boolean;
  isBotMessage: boolean;
} {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { text: raw, senderId: null, senderName: null, isFromMe: false, isBotMessage: false };
  }

  // WhatsApp native: {text, sender, senderId, is_from_me, is_bot_message}
  // Chat SDK (Discord/Slack): {text, author: {id, name}} or {text, sender, senderId}
  const text =
    typeof parsed.text === 'string' ? parsed.text : typeof parsed.content === 'string' ? parsed.content : raw;

  const senderId =
    typeof parsed.senderId === 'string'
      ? parsed.senderId
      : typeof parsed.sender_id === 'string'
        ? parsed.sender_id
        : typeof (parsed.author as Record<string, unknown>)?.id === 'string'
          ? String((parsed.author as Record<string, unknown>).id)
          : null;

  const senderName =
    typeof parsed.sender === 'string'
      ? parsed.sender
      : typeof parsed.sender_name === 'string'
        ? parsed.sender_name
        : typeof (parsed.author as Record<string, unknown>)?.name === 'string'
          ? String((parsed.author as Record<string, unknown>).name)
          : null;

  const isFromMe = parsed.is_from_me === true || parsed.is_from_me === 1;
  const isBotMessage = parsed.is_bot_message === true || parsed.is_bot_message === 1;

  return { text, senderId, senderName, isFromMe, isBotMessage };
}

export function archiveInboundMessage(event: InboundEvent): void {
  if (event.message.kind !== 'chat' && event.message.kind !== 'chat-sdk') return;

  try {
    const db = getArchiveDb();
    const { text, senderId, senderName, isFromMe, isBotMessage } = parseContent(event.message.content);

    _insert!.run({
      id: event.message.id,
      channel_type: event.channelType,
      platform_id: event.platformId,
      thread_id: event.threadId ?? null,
      timestamp: event.message.timestamp,
      sender_id: senderId,
      sender_name: senderName,
      content: text,
      is_from_me: isFromMe ? 1 : 0,
      is_bot_message: isBotMessage ? 1 : 0,
      is_group: event.message.isGroup ? 1 : 0,
      raw_content: event.message.content,
    });
  } catch (err) {
    // Never let archive errors disrupt message routing.
    log.warn('Message archive write failed', { err, id: event.message.id });
  }
}
