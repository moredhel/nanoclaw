/**
 * WhatsApp channel adapter (v2).
 *
 * Two registrations:
 *   'whatsapp'         — standard read+write, uses store/auth/
 *   'whatsapp-monitor' — read-only second connection, uses store/auth-monitor/
 *                        Suppresses all outbound sends and ownsJid always returns
 *                        false so the router never claims incoming messages on its
 *                        behalf. Used for monitoring a second number/device.
 *
 * Authentication: run `npm run auth` (standard) or `npm run auth:monitor` (monitor)
 * to scan a QR code. Credentials are persisted in the respective auth subdirectory.
 *
 * The monitor channel starts only when store/auth-monitor/creds.json exists.
 * If credentials expire (QR shown) the monitor channel fires onStatusChange('logged_out')
 * and terminates the socket rather than printing a QR code.
 */

import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';

import makeWASocket, {
  Browsers,
  DisconnectReason,
  downloadMediaMessage,
  WASocket,
  fetchLatestWaWebVersion,
  makeCacheableSignalKeyStore,
  useMultiFileAuthState,
} from '@whiskeysockets/baileys';

import { ASSISTANT_HAS_OWN_NUMBER, ASSISTANT_NAME, STORE_DIR } from '../config.js';
import { log } from '../log.js';
import type { ChannelAdapter, ChannelSetup, ChannelRegistration, OutboundMessage } from './adapter.js';
import { registerChannelAdapter } from './channel-registry.js';

const GROUP_SYNC_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 hours

// Baileys requires a pino-compatible logger. Wrap our built-in logger to satisfy
// the ILogger interface (level, child, trace).
const baileysLogger = {
  level: 'silent' as const,
  trace: () => {},
  debug: () => {},
  info: () => {},
  warn: (obj: Record<string, unknown>, msg?: string) => log.warn(msg ?? 'baileys warn', obj),
  error: (obj: Record<string, unknown>, msg?: string) => log.error(msg ?? 'baileys error', obj),
  fatal: (obj: Record<string, unknown>, msg?: string) => log.fatal(msg ?? 'baileys fatal', obj),
  child: () => baileysLogger,
};

interface WhatsAppAdapterOpts {
  authSubdir?: string;
  readOnly?: boolean;
  channelName?: string;
}

class WhatsAppChannel implements ChannelAdapter {
  name: string;
  channelType: string;
  supportsThreads = false as const;

  private sock!: WASocket;
  private connected = false;
  private notifiedDisconnect = false;
  private lidToPhoneMap: Record<string, string> = {};
  private outgoingQueue: Array<{ jid: string; text: string }> = [];
  private flushing = false;
  private groupSyncTimerStarted = false;
  private opts: WhatsAppAdapterOpts;
  private config!: ChannelSetup;

  constructor(channelType: string, opts: WhatsAppAdapterOpts = {}) {
    this.channelType = channelType;
    this.name = opts.channelName ?? channelType;
    this.opts = opts;
  }

  async setup(config: ChannelSetup): Promise<void> {
    this.config = config;
    return new Promise<void>((resolve, reject) => {
      this.connectInternal(resolve).catch(reject);
    });
  }

  private async connectInternal(onFirstOpen?: () => void): Promise<void> {
    const authDir = path.join(STORE_DIR, this.opts.authSubdir ?? 'auth');
    fs.mkdirSync(authDir, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(authDir);

    const { version } = await fetchLatestWaWebVersion({}).catch((err) => {
      log.warn('Failed to fetch latest WA Web version, using default', { err });
      return { version: undefined };
    });

    this.sock = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, baileysLogger),
      },
      printQRInTerminal: false,
      logger: baileysLogger,
      browser: Browsers.macOS('Chrome'),
    });

    this.sock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        if (this.opts.readOnly) {
          log.warn(`${this.name}: credentials expired, channel disabled.`);
          this.config.onStatusChange?.('logged_out', this.name);
          this.sock.end(undefined);
          return;
        }
        const msg = 'WhatsApp authentication required. Run /setup in Claude Code.';
        log.error(msg);
        exec(`osascript -e 'display notification "${msg}" with title "NanoClaw" sound name "Basso"'`);
        setTimeout(() => process.exit(1), 1000);
      }

      if (connection === 'close') {
        this.connected = false;
        const reason = (lastDisconnect?.error as { output?: { statusCode?: number } })?.output?.statusCode;
        const shouldReconnect = reason !== DisconnectReason.loggedOut;
        log.info('Connection closed', { reason, shouldReconnect, queuedMessages: this.outgoingQueue.length });

        if (shouldReconnect) {
          if (!this.notifiedDisconnect) {
            this.notifiedDisconnect = true;
            this.config.onStatusChange?.('disconnected', this.name);
          }
          this.scheduleReconnect(1);
        } else if (this.opts.readOnly) {
          log.warn(`${this.name} logged out — run npm run auth:monitor to re-authenticate.`);
          this.config.onStatusChange?.('logged_out', this.name);
        } else {
          log.error(`${this.name} logged out — run npm run auth to re-authenticate.`);
          this.config.onStatusChange?.('logged_out', this.name);
        }
      } else if (connection === 'open') {
        this.connected = true;
        this.notifiedDisconnect = false;
        this.config.onStatusChange?.('connected', this.name);
        log.info('Connected to WhatsApp', { channel: this.name });

        // Suppress push notifications on phone by staying "unavailable".
        // Per-chat typing indicators still work via sendPresenceUpdate.
        this.sock.sendPresenceUpdate('unavailable').catch((err) => {
          log.warn('Failed to send presence update', { err });
        });

        // Build LID-to-phone mapping from auth state for self-chat translation.
        if (this.sock.user) {
          const phoneUser = this.sock.user.id.split(':')[0];
          const lidUser = this.sock.user.lid?.split(':')[0];
          if (lidUser && phoneUser) {
            this.lidToPhoneMap[lidUser] = `${phoneUser}@s.whatsapp.net`;
            log.debug('LID to phone mapping set', { lidUser, phoneUser });
          }
        }

        this.flushOutgoingQueue().catch((err) => log.error('Failed to flush outgoing queue', { err }));

        // Sync group names on connect (respects 24h cache via timer).
        this.syncGroupNames().catch((err) => log.error('Initial group sync failed', { err }));
        if (!this.groupSyncTimerStarted) {
          this.groupSyncTimerStarted = true;
          setInterval(() => {
            this.syncGroupNames().catch((err) => log.error('Periodic group sync failed', { err }));
          }, GROUP_SYNC_INTERVAL_MS);
        }

        if (onFirstOpen) {
          onFirstOpen();
          onFirstOpen = undefined;
        }
      }
    });

    this.sock.ev.on('creds.update', saveCreds);

    this.sock.ev.on('messages.upsert', async ({ messages }) => {
      for (const msg of messages) {
        if (!msg.message) continue;
        const rawJid = msg.key.remoteJid;
        if (!rawJid || rawJid === 'status@broadcast') continue;

        const chatJid = await this.translateJid(rawJid);
        const isGroup = chatJid.endsWith('@g.us');
        const timestamp = new Date(Number(msg.messageTimestamp) * 1000).toISOString();

        // Notify host of chat metadata for group name discovery.
        this.config.onMetadata(chatJid, undefined, isGroup);

        let content =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message.imageMessage?.caption ||
          msg.message.videoMessage?.caption ||
          '';

        // Download and inline image content when available.
        if (!content && msg.message.imageMessage) {
          try {
            const buffer = await downloadMediaMessage(msg, 'buffer', {});
            const caption = msg.message.imageMessage.caption ?? '';
            // Pass as base64 data URI so the agent can display/describe it.
            const b64 = (buffer as Buffer).toString('base64');
            const mime = msg.message.imageMessage.mimetype ?? 'image/jpeg';
            content = caption
              ? `[Image: ${caption}]\ndata:${mime};base64,${b64}`
              : `[Image]\ndata:${mime};base64,${b64}`;
          } catch (err) {
            log.warn('Image download failed', { err, jid: chatJid });
            content = '[Image - download failed]';
          }
        }

        // Voice messages: note their presence (transcription not available in v2).
        if (!content && (msg.message.audioMessage || msg.message.ptvMessage)) {
          content = '[Voice Message]';
        }

        // Skip protocol messages with no text content.
        if (!content) continue;

        const sender = msg.key.participant || msg.key.remoteJid || '';
        const senderName = msg.pushName || sender.split('@')[0];
        const fromMe = msg.key.fromMe || false;
        const isBotMessage = ASSISTANT_HAS_OWN_NUMBER ? fromMe : content.startsWith(`${ASSISTANT_NAME}:`);

        await this.config.onInbound(chatJid, null, {
          id: msg.key.id || '',
          kind: 'chat',
          timestamp,
          content: {
            text: content,
            sender: senderName,
            senderId: sender,
            is_from_me: fromMe,
            is_bot_message: isBotMessage,
          },
        });
      }
    });
  }

  async teardown(): Promise<void> {
    this.connected = false;
    this.sock?.end(undefined);
  }

  isConnected(): boolean {
    if (this.opts.readOnly) return false; // monitor never "owns" messages
    return this.connected;
  }

  async deliver(platformId: string, _threadId: string | null, message: OutboundMessage): Promise<string | undefined> {
    if (this.opts.readOnly) {
      log.debug('Monitor WA: outbound suppressed', { jid: platformId });
      return undefined;
    }

    const content = message.content as Record<string, unknown> | string | undefined;
    const rawText = typeof content === 'string' ? content : typeof content?.text === 'string' ? content.text : null;
    if (rawText === null) return undefined;

    const prefixed = ASSISTANT_HAS_OWN_NUMBER ? rawText : `${ASSISTANT_NAME}: ${rawText}`;

    if (!this.connected) {
      this.outgoingQueue.push({ jid: platformId, text: prefixed });
      log.info('WA disconnected, message queued', { jid: platformId, queueSize: this.outgoingQueue.length });
      return undefined;
    }
    try {
      await this.sock.sendMessage(platformId, { text: prefixed });
      log.info('Message sent', { jid: platformId, length: prefixed.length });
    } catch (err) {
      this.outgoingQueue.push({ jid: platformId, text: prefixed });
      log.warn('Failed to send, message queued', { jid: platformId, err, queueSize: this.outgoingQueue.length });
    }
    return undefined;
  }

  async setTyping(platformId: string, _threadId: string | null): Promise<void> {
    try {
      await this.sock.sendPresenceUpdate('composing', platformId);
    } catch (err) {
      log.debug('Failed to send typing indicator', { jid: platformId, err });
    }
  }

  private async syncGroupNames(): Promise<void> {
    try {
      const groups = await this.sock.groupFetchAllParticipating();
      for (const [jid, metadata] of Object.entries(groups)) {
        if (metadata.subject) {
          this.config.onMetadata(jid, metadata.subject, true);
        }
      }
      log.info('Group metadata synced', { count: Object.keys(groups).length });
    } catch (err) {
      log.error('Failed to sync group metadata', { err });
    }
  }

  private scheduleReconnect(attempt: number): void {
    const delayMs = Math.min(5000 * Math.pow(2, attempt - 1), 300000);
    log.info('Reconnecting...', { attempt, delayMs });
    setTimeout(() => {
      this.connectInternal().catch((err) => {
        log.error('Reconnection attempt failed', { err, attempt });
        this.scheduleReconnect(attempt + 1);
      });
    }, delayMs);
  }

  private async translateJid(jid: string): Promise<string> {
    if (!jid.endsWith('@lid')) return jid;
    const lidUser = jid.split('@')[0].split(':')[0];

    const cached = this.lidToPhoneMap[lidUser];
    if (cached) {
      log.debug('Translated LID to phone JID (cached)', { lidJid: jid, phoneJid: cached });
      return cached;
    }

    try {
      const pn = await this.sock.signalRepository?.lidMapping?.getPNForLID(jid);
      if (pn) {
        const phoneJid = `${pn.split('@')[0].split(':')[0]}@s.whatsapp.net`;
        this.lidToPhoneMap[lidUser] = phoneJid;
        log.info('Translated LID to phone JID (signalRepository)', { lidJid: jid, phoneJid });
        return phoneJid;
      }
    } catch (err) {
      log.debug('Failed to resolve LID via signalRepository', { err, jid });
    }

    return jid;
  }

  private async flushOutgoingQueue(): Promise<void> {
    if (this.flushing || this.outgoingQueue.length === 0) return;
    this.flushing = true;
    try {
      log.info('Flushing outgoing message queue', { count: this.outgoingQueue.length });
      while (this.outgoingQueue.length > 0) {
        const item = this.outgoingQueue.shift()!;
        await this.sock.sendMessage(item.jid, { text: item.text });
        log.info('Queued message sent', { jid: item.jid, length: item.text.length });
      }
    } finally {
      this.flushing = false;
    }
  }
}

// Standard read+write WhatsApp channel.
registerChannelAdapter('whatsapp', {
  factory: () => {
    const authDir = path.join(STORE_DIR, 'auth');
    if (!fs.existsSync(path.join(authDir, 'creds.json'))) {
      log.warn('WhatsApp: credentials not found. Run /add-whatsapp to authenticate.');
      return null;
    }
    return new WhatsAppChannel('whatsapp');
  },
} satisfies ChannelRegistration);

// Read-only monitor connection — only active when its own credentials exist.
registerChannelAdapter('whatsapp-monitor', {
  factory: () => {
    const authDir = path.join(STORE_DIR, 'auth-monitor');
    if (!fs.existsSync(path.join(authDir, 'creds.json'))) {
      return null;
    }
    return new WhatsAppChannel('whatsapp-monitor', {
      authSubdir: 'auth-monitor',
      readOnly: true,
      channelName: 'whatsapp-monitor',
    });
  },
} satisfies ChannelRegistration);
