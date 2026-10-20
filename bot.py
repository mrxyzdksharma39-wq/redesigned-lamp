#!/usr/bin/env python3
"""Single-owner Telegram copier. Python 3.12+. See README before deploying.
Run embedded offline tests: python bot.py --self-test
No plaintext Telethon session files are created.
"""
from __future__ import annotations
import argparse
import asyncio
import contextlib
import copy
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, functions, types, utils
from telethon.sessions import StringSession
from telethon.helpers import add_surrogate, del_surrogate

LOG = logging.getLogger('copier')
DEFAULTS = {'replace': False, 'rules': {}, 'links': 'keep', 'domains': [],
            'notify_success': False, 'duration': 0, 'mode': 'account', 'destination': None}
MENU = [('Connect Account', 'connect'), ('My Connected Account', 'account'),
        ('Search Source Channel', 'search'), ('Add Source Channel', 'add'),
        ('My Source Channels', 'sources'), ('Set Destination Channel', 'destination'),
        ('Copy Settings', 'settings'), ('Start All', 'start'), ('Stop All', 'stop'),
        ('Dashboard & Statistics', 'stats'), ('Error Logs', 'logs'),
        ('Security & Sessions', 'security'), ('Help & Setup Guide', 'help')]
HINDI = ('API credentials केवल https://my.telegram.org से लें।\n'
         '1. वेबसाइट खोलें और अपने Telegram नंबर से sign in करें।\n'
         '2. Telegram का authentication पूरा करें।\n'
         '3. API development tools खोलें।\n'
         '4. जरूरत हो तो application बनाएं।\n'
         '5. API ID और API HASH लें, फिर यहां लौटें।\n'
         'यह केवल आपके अपने self-hosted bot के लिए है। अन्य bots को login secrets न दें।\n'
         'Bot chats end-to-end encrypted नहीं हैं; sensitive messages best-effort delete होंगे।')


class UserError(Exception):
    """An intentionally safe, user-facing error."""


class BotError(Exception):
    def __init__(self, code, retry_after=0):
        self.code, self.retry_after = code, retry_after
        super().__init__(f'Bot API error {code}')


def keyboard(items, back=True):
    rows = [[{'text': label, 'callback_data': data}] for label, data in items]
    if back:
        rows.append([{'text': 'Back / Cancel', 'callback_data': 'home'}])
    return {'inline_keyboard': rows}


def domain_of(value):
    try:
        return (urlparse(value if '://' in value else 'https://' + value).hostname or '').lower().rstrip('.')
    except ValueError:
        return ''


def transform(text, entities, settings):
    """Edit UTF-16 offsets, not Python code-point offsets. Never edit inside URLs."""
    text = add_surrogate(text or '')
    entities = copy.deepcopy(entities or [])
    edits, occupied = [], []
    urls = []
    for ent in entities:
        if isinstance(ent, (types.MessageEntityUrl, types.MessageEntityTextUrl)):
            a, b = ent.offset, ent.offset + ent.length
            url = getattr(ent, 'url', None) or del_surrogate(text[a:b])
            urls.append((a, b, url))
    for match in re.finditer(r'(?i)\b(?:https?://|www\.|t\.me/)[^\s<>]+', text):
        if not any(a <= match.start() < b for a, b, _ in urls):
            urls.append((match.start(), match.end(), del_surrogate(match.group())))
    for a, b, url in urls:
        occupied.append((a, b))
        host = domain_of(url)
        listed = any(host == d or host.endswith('.' + d) for d in settings['domains'])
        mode = settings['links']
        if mode == 'remove' or (mode == 'block' and listed) or (mode == 'allow' and not listed):
            edits.append((a, b, ''))
    if settings['replace']:
        for match in re.finditer(r'(?<![\w@/])@[A-Za-z0-9_]{5,32}\b', text):
            if any(a < match.end() and b > match.start() for a, b in occupied):
                continue
            replacement = settings['rules'].get(match.group().lower())
            if replacement:
                edits.append((match.start(), match.end(), add_surrogate(replacement)))
    # Discard overlapping URL edits, then apply from right to left.
    selected = []
    for edit in sorted(edits, key=lambda x: (x[0], -x[1])):
        if not selected or edit[0] >= selected[-1][1]:
            selected.append(edit)
    for a, b, value in reversed(selected):
        delta = len(value) - (b - a)
        new_entities = []
        for ent in entities:
            start, end = ent.offset, ent.offset + ent.length
            if end <= a:
                pass
            elif start >= b:
                ent.offset += delta
            elif start <= a and end >= b:
                ent.length += delta
            else:
                # An entity partly cut by an edit is unsafe to preserve.
                continue
            if ent.length > 0:
                new_entities.append(ent)
        entities = new_entities
        text = text[:a] + value + text[b:]
    return del_surrogate(text), entities


def bot_entities(entities):
    mapping = {'Bold': 'bold', 'Italic': 'italic', 'Underline': 'underline',
               'Strike': 'strikethrough', 'Code': 'code', 'Pre': 'pre',
               'TextUrl': 'text_link', 'Url': 'url', 'Mention': 'mention',
               'Hashtag': 'hashtag', 'Cashtag': 'cashtag', 'Email': 'email',
               'Phone': 'phone_number', 'BotCommand': 'bot_command',
               'Spoiler': 'spoiler', 'Blockquote': 'blockquote', 'CustomEmoji': 'custom_emoji'}
    result = []
    for ent in entities:
        name = type(ent).__name__.removeprefix('MessageEntity')
        if name == 'MentionName':
            result.append({'type': 'text_link', 'offset': ent.offset, 'length': ent.length,
                           'url': f'tg://user?id={ent.user_id}'})
            continue
        if name not in mapping:
            raise UserError(f'Unsupported text entity: {name}; post was not published.')
        item = {'type': mapping[name], 'offset': ent.offset, 'length': ent.length}
        if name == 'TextUrl':
            item['url'] = ent.url
        if name == 'Pre':
            item['language'] = ent.language
        if name == 'CustomEmoji':
            item['custom_emoji_id'] = str(ent.document_id)
        if name == 'Blockquote' and getattr(ent, 'collapsed', False):
            item['type'] = 'expandable_blockquote'
        result.append(item)
    return result


class Store:
    def __init__(self, path, key):
        self.path = str(path)
        self.fernet = Fernet(key)
        self.db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA secure_delete=ON')
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version > 1:
            raise RuntimeError('Database is newer than this application')
        self.db.executescript('''
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS secrets(k TEXT PRIMARY KEY, v BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY, peer BLOB NOT NULL, title TEXT NOT NULL,
          username TEXT, dest BLOB, mode TEXT NOT NULL DEFAULT 'account',
          enabled INTEGER NOT NULL DEFAULT 0, since REAL NOT NULL DEFAULT 0,
          until REAL NOT NULL DEFAULT 0, last INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'Ready');
        CREATE TABLE IF NOT EXISTS jobs(
          source INTEGER NOT NULL, key TEXT NOT NULL, first_id INTEGER NOT NULL,
          payload BLOB NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
          attempt INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL,
          created REAL NOT NULL, error TEXT,
          PRIMARY KEY(source,key));
        CREATE TABLE IF NOT EXISTS mappings(
          source INTEGER NOT NULL, msg INTEGER NOT NULL, dest INTEGER NOT NULL,
          mode TEXT NOT NULL, sent INTEGER NOT NULL,
          PRIMARY KEY(source,msg,dest,mode));
        CREATE TABLE IF NOT EXISTS logs(
          id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, category TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS jobs_dispatch ON jobs(status,due,created);
        CREATE INDEX IF NOT EXISTS jobs_source_order ON jobs(source,first_id,status);
        PRAGMA user_version=1;
        COMMIT;
        ''')
        check = self.get('keycheck')
        if check is None:
            self.set('keycheck', 'ok')
        elif check != 'ok':
            raise RuntimeError('Invalid encryption key')
        os.chmod(self.path, 0o600)
        self.db.execute("UPDATE jobs SET status='uncertain', error='Restart during publication' WHERE status='sending'")

    def seal(self, value):
        return self.fernet.encrypt(json.dumps(value, ensure_ascii=False).encode())

    def open(self, value):
        return json.loads(self.fernet.decrypt(value)) if value is not None else None

    def get(self, key, default=None):
        row = self.db.execute('SELECT v FROM secrets WHERE k=?', (key,)).fetchone()
        return self.open(row[0]) if row else default

    def set(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO secrets VALUES(?,?)', (key, self.seal(value)))

    def delete(self, key):
        self.db.execute('DELETE FROM secrets WHERE k=?', (key,))

    def rows(self, sql, args=()):
        return self.db.execute(sql, args).fetchall()

    def execute(self, sql, args=()):
        return self.db.execute(sql, args)

    @contextlib.contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def log(self, category):
        # Only controlled labels / exception class names; never exception text.
        self.execute('INSERT INTO logs(at,category) VALUES(?,?)', (time.time(), category[:160]))
        self.execute('DELETE FROM logs WHERE id < (SELECT COALESCE(MAX(id),0)-1000 FROM logs)')
        LOG.warning('%s', category[:160])

    def close(self):
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.db.close()


class BotAPI:
    def __init__(self, session, token):
        self.session = session
        self.url = 'https://api.telegram.org/bot' + token + '/'

    async def call(self, method, data=None, files=None):
        data = data or {}
        form = None
        if files:
            form = aiohttp.FormData()
            for key, value in data.items():
                form.add_field(key, json.dumps(value) if isinstance(value, (dict, list, bool)) else str(value))
            for key, (filename, content) in files.items():
                form.add_field(key, content, filename=filename, content_type='application/octet-stream')
        async with self.session.post(self.url + method, data=form,
                                     json=None if form is not None else data) as response:
            result = await response.json(content_type=None)
        if not result.get('ok'):
            raise BotError(result.get('error_code', 500), result.get('parameters', {}).get('retry_after', 0))
        return result['result']


class Application:
    def __init__(self, db, bot, owner, data_dir):
        self.db, self.bot, self.owner, self.data_dir = db, bot, owner, data_dir
        self.client = None
        self.me = None
        self.bot_id = None
        self.settings = {**copy.deepcopy(DEFAULTS), **db.get('settings', {})}
        self.flow = db.get('flow', {})
        self.closing = asyncio.Event()
        self.publish_lock = asyncio.Lock()
        self.admin_lock = asyncio.Lock()
        self.next_checks = 0
        self.wake = asyncio.Event()
        self.tasks = []
        self.ready = False
        self.last_poll = time.time()
        self.last_notice = {}
        self.pending_limit = int(os.getenv('MAX_PENDING_JOBS', '5000'))
        self.max_media = int(os.getenv('MAX_MEDIA_MB', '45')) * 1024 * 1024
        self.auth_ttl = int(os.getenv('AUTH_TTL_SECONDS', '600'))
        if not (1 <= self.pending_limit <= 100000 and 1 <= self.max_media <= 49 * 1024 * 1024 and 60 <= self.auth_ttl <= 1800):
            raise RuntimeError('Invalid queue, media, or authentication limits')

    def save_settings(self):
        self.db.set('settings', self.settings)

    def set_flow(self, stage=None, **values):
        self.flow = dict(stage=stage, expires=time.time()+self.auth_ttl, **values) if stage else {}
        self.db.set('flow', self.flow)

    async def say(self, text, buttons=None):
        return await self.bot.call('sendMessage', {'chat_id': self.owner, 'text': text[:4000],
                                   'reply_markup': keyboard(buttons or []),
                                   'link_preview_options': {'is_disabled': True}})

    async def notice(self, key, text, interval=60):
        if time.time() - self.last_notice.get(key, 0) >= interval:
            self.last_notice[key] = time.time()
            try:
                await self.say(text)
            except (BotError, aiohttp.ClientError, asyncio.TimeoutError):
                self.db.log('OwnerNotificationUnavailable')

    def active(self):
        if not self.client or not self.me:
            raise UserError('Connect your Telegram account first.')
        if not self.client.is_connected():
            raise UserError('Telegram connection is recovering. Try again shortly.')
        return self.client

    def peer_record(self, entity):
        return {'id': entity.id, 'hash': entity.access_hash,
                'chat': utils.get_peer_id(entity), 'title': entity.title}

    def peer(self, record):
        return types.InputPeerChannel(record['id'], record['hash'])

    async def new_client(self, session=''):
        credentials = self.db.get('credentials')
        if not credentials:
            raise UserError('Configure API credentials first.')
        client = TelegramClient(StringSession(session), credentials['id'], credentials['hash'],
                                flood_sleep_threshold=0, request_retries=0,
                                connection_retries=5, retry_delay=2, auto_reconnect=True,
                                sequential_updates=True, catch_up=False)
        client.parse_mode = None
        await client.connect()
        return client

    async def attach(self):
        self.client.add_event_handler(self.on_message, events.NewMessage())
        self.client.add_event_handler(self.on_album, events.Album())

    async def resume(self):
        auth = self.db.get('pending')
        if auth and auth['expires'] <= time.time():
            self.db.delete('pending')
            self.set_flow()
        saved = self.db.get('session')
        if saved:
            try:
                self.client = await self.new_client(saved)
                if not await self.client.is_user_authorized():
                    raise UserError('Saved session is no longer authorized.')
                self.me = await self.client.get_me()
                if not self.me or self.me.id != self.owner:
                    raise UserError('Saved session does not belong to OWNER_ID.')
                await self.attach()
                for row in self.db.rows('SELECT * FROM sources WHERE enabled=1'):
                    try:
                        if row['until'] and row['until'] <= time.time():
                            raise UserError('Duration expired')
                        await self.check_source(row)
                        await self.check_destination(self.db.open(row['dest']), row['mode'])
                    except Exception as exc:
                        self.db.execute("UPDATE sources SET enabled=0,status=? WHERE id=?",
                                        (type(exc).__name__, row['id']))
                await self.notice('recovery', 'Encrypted session restored; saved monitoring permissions rechecked.')
            except (errors.RPCError, UserError) as exc:
                self.me = None
                self.db.execute("UPDATE sources SET enabled=0,status='Reauthentication required'")
                self.db.log('SessionRecovery:' + type(exc).__name__)
                if self.client:
                    await self.client.disconnect()
                self.client = None
                await self.notice('auth', 'Saved session could not be authorized. Monitoring is paused; reconnect your account.')

    async def cancel_auth(self):
        if self.client and not self.me:
            pending = self.db.get('pending')
            if pending and pending.get('phone') and pending.get('code_hash'):
                with contextlib.suppress(errors.RPCError, ConnectionError):
                    await self.client(functions.auth.CancelCodeRequest(pending['phone'], pending['code_hash']))
            await self.client.disconnect()
            self.client = None
        self.db.delete('pending')
        self.set_flow()

    async def pending_client(self):
        pending = self.db.get('pending')
        if not pending or pending['expires'] <= time.time():
            await self.cancel_auth()
            raise UserError('Authentication expired. Start Connect Account again.')
        if not self.client:
            self.client = await self.new_client(pending['session'])
        return pending

    async def finish_auth(self):
        me = await self.client.get_me()
        if not me or me.id != self.owner:
            await self.client.log_out()
            self.client = None
            self.db.delete('pending')
            self.set_flow()
            raise UserError('The connected account must match OWNER_ID. That session was logged out.')
        session = self.client.session.save()
        self.db.set('session', session)
        await self.client.disconnect()
        self.client = await self.new_client(self.db.get('session'))
        if not await self.client.is_user_authorized():
            raise UserError('Session saved, but reconnect verification failed. Reconnect before monitoring.')
        self.me = await self.client.get_me()
        self.db.set('account', {'id': self.me.id, 'phone': '***' + (self.me.phone or '')[-4:]})
        self.db.delete('pending')
        self.set_flow()
        await self.attach()
        await self.say('Telegram account connected.\nSession: encrypted and stored\nConnection: verified',
                       [('Search Channels', 'search'), ('Add Source', 'add'), ('Session Status', 'security')])

    async def resolve(self, value, join=False):
        client = self.active()
        value = value.strip()
        match = re.fullmatch(r'(?:https?://)?(?:t\.me|telegram\.me)/(?:\+|joinchat/)([\w-]+)(?:\?.*)?', value)
        if match:
            invite = await client(functions.messages.CheckChatInviteRequest(match[1]))
            if isinstance(invite, types.ChatInviteAlready):
                entity = invite.chat
            elif not join:
                raise UserError('Private invite is valid, but membership is not confirmed. Use Add Source to request joining.')
            else:
                if not getattr(invite, 'broadcast', False) and not getattr(getattr(invite, 'chat', None), 'broadcast', False):
                    raise UserError('This invitation is not for a broadcast channel.')
                try:
                    result = await client(functions.messages.ImportChatInviteRequest(match[1]))
                    entity = result.chats[0]
                except errors.InviteRequestSentError:
                    joins = self.db.get('joins', {})
                    if len(joins) >= 20 and match[1] not in joins:
                        raise UserError('Join request sent; pending-request tracker is full. Recheck manually with Add Source after approval.')
                    joins[match[1]] = {'requested': time.time(), 'title': getattr(invite, 'title', 'Private channel')}
                    self.db.set('joins', joins)
                    self.set_flow()
                    raise UserError('Join Request Pending. Membership will be rechecked periodically. If approved, the source is added OFF; you must configure and start it.')
        else:
            public = re.fullmatch(r'(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][\w]{3,31})/?', value)
            if public:
                value = '@' + public[1]
            if re.fullmatch(r'-100\d+', value):
                target = int(value)
                entity = None
                async for dialog in client.iter_dialogs():
                    if dialog.id == target:
                        entity = dialog.entity
                        break
                if entity is None:
                    raise UserError('That channel ID is not in this account\'s accessible dialogs.')
            else:
                if not re.fullmatch(r'@?[A-Za-z][\w]{3,31}', value):
                    raise UserError('Use an exact @username, public channel URL, invite URL or accessible -100 channel ID. Display-name search is ambiguous.')
                entity = await client.get_entity(value)
            if isinstance(entity, types.Chat) or not isinstance(entity, types.Channel):
                raise UserError('This is not a broadcast channel. Groups and forum destinations are not supported.')
            if join and entity.broadcast and getattr(entity, 'left', False):
                await client(functions.channels.JoinChannelRequest(entity))
                entity = await client.get_entity(entity)
        if isinstance(entity, types.Channel) and getattr(entity, 'left', False):
            if join:
                raise UserError('Membership is not confirmed. Wait for approval, then add the source again.')
        return entity

    def register_source(self, entity):
        if entity.noforwards:
            raise UserError('This channel protects its content. It cannot be copied.')
        sid = utils.get_peer_id(entity)
        if self.db.rows('SELECT COUNT(*) FROM sources')[0][0] >= 80 and not self.db.rows('SELECT 1 FROM sources WHERE id=?', (sid,)):
            raise UserError('This installation supports at most 80 configured sources.')
        destinations = [self.db.open(r['dest']) for r in self.db.rows('SELECT dest FROM sources WHERE dest IS NOT NULL')]
        destinations.append(self.settings['destination'])
        if any(d and d['chat'] == sid for d in destinations):
            raise UserError('This channel is already a destination. Copy loops are prohibited.')
        self.db.execute('INSERT INTO sources(id,peer,title,username,dest,mode) VALUES(?,?,?,?,?,?) '
                        'ON CONFLICT(id) DO UPDATE SET peer=excluded.peer,title=excluded.title,username=excluded.username',
                        (sid, self.db.seal(self.peer_record(entity)), entity.title, entity.username,
                         self.db.seal(self.settings['destination']) if self.settings['destination'] else None,
                         self.settings['mode']))
        return sid

    async def resolve_peer(self, peer):
        self.active()
        try:
            return await self.client.get_entity(peer)
        except errors.RPCError:
            async for dialog in self.client.iter_dialogs():
                if dialog.entity and utils.get_peer_id(dialog.entity) == peer.chat_id:
                    return dialog.entity
            raise

    async def check_source(self, row):
        entity = await self.resolve_peer(self.peer(self.db.open(row['peer'])))
        if getattr(entity, 'left', False) or getattr(entity, 'noforwards', False):
            raise UserError('Source is inaccessible or has protected content; copying is disabled.')
        self.db.execute("UPDATE sources SET title=?,username=?,status='Member / accessible' WHERE id=?",
                        (entity.title, entity.username, row['id']))
        return entity

    async def check_destination(self, dest, mode):
        if not dest:
            raise UserError('Configure a destination first.')
        if mode == 'bot':
            member = await self.bot.call('getChatMember', {'chat_id': dest['chat'], 'user_id': self.bot_id})
            if member['status'] != 'creator' and not (member['status'] == 'administrator' and member.get('can_post_messages')):
                raise UserError('Bot must be a destination administrator with Post Messages permission.')
        else:
            entity = await self.resolve_peer(self.peer(dest))
            if not entity.creator and not (entity.admin_rights and entity.admin_rights.post_messages):
                raise UserError('Your account lacks permission to post in the destination.')
        return dest

    async def home(self):
        count = self.db.rows('SELECT COUNT(*),SUM(enabled) FROM sources')[0]
        await self.say(f'Telegram Auto-Copy Manager\nAccount: {"Connected" if self.me else "Not connected"}\n'
                       f'Source Channels: {count[0]}\nDestination: {"Configured" if self.settings["destination"] else "Not set"}\n'
                       f'Monitoring: {count[1] or 0} sources ON', MENU)

    async def stop(self, source=None):
        # Lock lets an already-running send finish, then atomically stops queued work.
        async with self.publish_lock:
            clause, args = (' WHERE id=?', (source,)) if source else ('', ())
            self.db.execute("UPDATE sources SET enabled=0,status='Stopped'" + clause, args)
            clause, args = (' AND source=?', (source,)) if source else ('', ())
            self.db.execute("UPDATE jobs SET status='cancelled' WHERE status='queued'" + clause, args)

    async def start(self, source=None):
        self.active()
        rows = self.db.rows('SELECT * FROM sources' + (' WHERE id=?' if source else ''), (source,) if source else ())
        if not rows:
            raise UserError('Add at least one source first.')
        for row in rows:
            if self.db.rows("SELECT 1 FROM jobs WHERE source=? AND status IN ('failed','uncertain') LIMIT 1", (row['id'],)):
                raise UserError('A failed or uncertain job blocks this source. Inspect /blocked and the destination, then /skip the specific job before starting.')
            await self.check_source(row)
            dest = await self.check_destination(self.db.open(row['dest']), row['mode'])
            if self.db.rows('SELECT 1 FROM sources WHERE id=?', (dest['chat'],)):
                raise UserError('A destination is also a source. Remove that source to prevent copy loops.')
        async with self.publish_lock:
            now = time.time()
            for row in rows:
                if not row['enabled']:
                    duration = self.settings['duration']
                    self.db.execute("UPDATE sources SET enabled=1,since=?,until=?,status='Monitoring' WHERE id=?",
                                    (now, now + duration if duration else 0, row['id']))
        self.wake.set()

    async def source_list(self):
        rows = self.db.rows('SELECT * FROM sources ORDER BY title')
        await self.say('Source channels (use exact IDs shown in details).',
                       [(f'{r["title"][:45]} [{"ON" if r["enabled"] else "OFF"}]', f'src:{r["id"]}') for r in rows[:80]])

    async def source_detail(self, sid):
        rows = self.db.rows('SELECT * FROM sources WHERE id=?', (sid,))
        if not rows:
            raise UserError('Source no longer exists.')
        r = rows[0]
        dest = self.db.open(r['dest'])
        await self.say(f'{r["title"]}\nUsername: {r["username"] or "private"}\nID: {sid}\n'
                       f'Status: {r["status"]}\nCopying: {bool(r["enabled"])}\n'
                       f'Destination: {dest["title"] if dest else "Not configured"} ({r["mode"]})\n'
                       f'Last processed: {r["last"]}',
                       [('Start', f'on:{sid}'), ('Stop', f'off:{sid}'), ('Change Destination', f'dest:{sid}'),
                        ('Refresh Access', f'check:{sid}'), ('Remove Source', f'remove:{sid}'), ('Sources', 'sources')])

    async def settings_menu(self):
        s = self.settings
        await self.say(f'Copy Settings\nReplacement: {s["replace"]} ({len(s["rules"])} rules)\n'
                       f'Links: {s["links"]}\nDomains: {", ".join(s["domains"]) or "none"}\n'
                       f'Duration for next start: {s["duration"]} seconds (0 = unlimited)\n'
                       f'Success notifications: {s["notify_success"]}\nDefault publisher: {s["mode"]}',
                       [('Toggle Replacement', 'replace'), ('Add / Update Rule', 'rule'),
                        ('View / Delete Rules', 'rules'), ('Test Transformation', 'test'),
                        ('Keep All Links', 'links:keep'), ('Remove URLs', 'links:remove'),
                        ('Block Domains', 'links:block'), ('Allow Only Domains', 'links:allow'),
                        ('Set Domain List', 'domains'), ('Duration', 'duration'),
                        ('Toggle Success Notifications', 'notify'), ('Bot API Mode', 'mode:bot'),
                        ('Authorized Account Mode', 'mode:account')])

    async def callback(self, data):
        action, _, arg = data.partition(':')
        privileged = {'account', 'security', 'search', 'add', 'sources', 'src', 'check', 'on', 'off', 'remove',
                     'remove_yes', 'dest', 'destination', 'start', 'stats', 'blocked', 'connect', 'continue', 'resend'}
        if action in privileged and not self.db.get('session') and action not in ('connect', 'continue', 'resend'):
            raise UserError('Connect your Telegram account first.')
        auth_actions = ('connect', 'continue', 'resend')
        if action in auth_actions and self.me:
            raise UserError('Already connected. Disconnect before starting authentication.')
        if action == 'continue' and self.flow.get('stage') != 'confirm':
            raise UserError('This login button has expired. Use Connect Account.')
        if action == 'resend' and self.flow.get('stage') != 'otp':
            raise UserError('There is no pending code step.')
        if action == 'home':
            if self.flow.get('stage') in ('phone', 'otp', 'password', 'api_id', 'api_hash', 'confirm'):
                await self.cancel_auth()
            else:
                self.set_flow()
            return await self.home()
        if action == 'connect':
            if self.me:
                raise UserError('Already connected. Disconnect before changing API credentials.')
            if self.flow.get('stage') in ('api_id', 'api_hash', 'confirm', 'phone', 'otp', 'password') and self.flow.get('expires', 0) > time.time():
                await self.say('Pending step: ' + str(self.flow.get('stage')) + '. Enter its requested value, or cancel.', [('Cancel Setup', 'home')])
                return
            if self.db.get('credentials'):
                self.set_flow('phone')
                return await self.say('API credentials are configured. अपना नंबर country code सहित भेजें, जैसे +91XXXXXXXXXX।')
            self.set_flow('api_id')
            await self.say(HINDI + '\n\nअब numeric API ID भेजें।')
            return await self.bot.call('sendMessage', {'chat_id': self.owner, 'text': 'Official Telegram developer portal',
                'reply_markup': {'inline_keyboard': [[{'text': 'Open Official Website', 'url': 'https://my.telegram.org'}]]}})
        if action in ('account', 'security'):
            account = self.db.get('account', {})
            await self.say(f'Account: {account.get("phone", "Not connected")}\n'
                           f'Authorized: {bool(self.me)}\nTransport connected: {bool(self.client and self.client.is_connected())}\n'
                           f'Encrypted stored session: {bool(self.db.get("session"))}\n'
                           'Disconnect deletes local session and account-specific history. Revoke also terminates this Telegram session. API credentials are retained.',
                           [('Disconnect Locally', 'confirm_disconnect'), ('Revoke & Disconnect', 'confirm_revoke'),
                            ('Reset API Credentials', 'confirm_reset')])
        elif action in ('confirm_disconnect', 'confirm_revoke', 'confirm_reset'):
            await self.say('Confirm this destructive action. Monitoring will stop. Credential reset requires a disconnected account.',
                           [('Confirm', action.removeprefix('confirm_')), ('Cancel', 'home')])
        elif action in ('disconnect', 'revoke'):
            await self.disconnect(revoke=action == 'revoke')
        elif action == 'reset':
            if self.me or self.db.get('session'):
                raise UserError('Disconnect first. Reset affects the sole account in this installation.')
            await self.cancel_auth()
            self.db.delete('credentials')
            await self.say('API credentials deleted. Encryption key was not changed.')
        elif action == 'continue':
            self.set_flow('phone')
            await self.say('अपना Telegram phone number country code सहित भेजें, जैसे +91XXXXXXXXXX।')
        elif action == 'resend':
            p = await self.pending_client()
            if time.time() < p['resend_at'] or not p.get('can_resend'):
                raise UserError('Telegram has not allowed a resend yet. Wait or cancel and restart login.')
            result = await self.client(functions.auth.ResendCodeRequest(p['phone'], p['code_hash']))
            await self.save_code(p['phone'], result)
        elif action in ('search', 'add'):
            self.active()
            self.set_flow(action)
            await self.say('Send an exact @channel, https://t.me/channel, private invite link, or accessible -100 channel ID. Add Source joins when permitted; search does not join.')
        elif action == 'sources':
            await self.source_list()
        elif action in ('src', 'check', 'on', 'off', 'remove', 'remove_yes', 'dest'):
            sid = int(arg)
            rows = self.db.rows('SELECT * FROM sources WHERE id=?', (sid,))
            if not rows:
                raise UserError('Source no longer exists.')
            if action == 'check':
                await self.check_source(rows[0])
            elif action == 'on':
                await self.start(sid)
            elif action == 'off':
                await self.stop(sid)
            elif action == 'remove':
                return await self.say('Remove this source and stop its queued jobs?', [('Confirm Remove', f'remove_yes:{sid}')])
            elif action == 'remove_yes':
                await self.stop(sid)
                self.db.execute('DELETE FROM sources WHERE id=?', (sid,))
                return await self.source_list()
            elif action == 'dest':
                self.set_flow('destination', source=sid)
                return await self.say('Send destination @username or accessible channel link. Uses the selected default publisher mode. Stop this source first.')
            await self.source_detail(sid)
        elif action == 'destination':
            self.set_flow('destination')
            await self.say('Send the destination @username, channel link or accessible ID. This sets the default for future sources and sources without a destination. Existing mappings are unchanged.')
        elif action in ('start', 'stop'):
            await (self.start() if action == 'start' else self.stop())
            await self.say('Monitoring started for configured sources.' if action == 'start' else 'Monitoring stopped. In-flight send finished; unsent queued jobs cancelled.')
        elif action == 'settings':
            await self.settings_menu()
        elif action in ('replace', 'notify'):
            key = 'replace' if action == 'replace' else 'notify_success'
            self.settings[key] = not self.settings[key]
            self.save_settings()
            await self.settings_menu()
        elif action == 'links':
            if arg not in ('keep', 'remove', 'block', 'allow'):
                raise UserError('Invalid filter mode.')
            self.settings['links'] = arg
            self.save_settings()
            await self.settings_menu()
        elif action == 'mode':
            if arg not in ('bot', 'account'):
                raise UserError('Invalid publisher mode.')
            self.settings['mode'] = arg
            self.save_settings()
            await self.say('Default publisher updated for the next destination assignment. Bot mode requires bot admin rights and uploads downloaded media; account mode uses your account and reusable media references. Existing mappings are unchanged.')
        elif action in ('rule', 'domains', 'duration', 'test'):
            self.set_flow(action)
            await self.say({'rule': 'Send @original @replacement (exact username matches).',
                            'domains': 'Send comma-separated domains; send - to clear.',
                            'duration': 'Send duration in seconds, 0 for unlimited. Applies on the next OFF-to-ON transition.',
                            'test': 'Send sample text to preview transformations; this does not publish.'}[action])
        elif action == 'rules':
            rules = self.settings['rules']
            await self.say('\n'.join(f'{a} -> {b}' for a, b in rules.items()) or 'No rules.',
                           [('Delete ' + a, 'delrule:' + a) for a in rules])
        elif action == 'delrule':
            self.settings['rules'].pop(arg, None)
            self.save_settings()
            await self.settings_menu()
        elif action == 'stats':
            rows = self.db.rows('SELECT status,COUNT(*) AS n FROM jobs GROUP BY status')
            await self.say('Publishing jobs\n' + '\n'.join(f'{r["status"]}: {r["n"]}' for r in rows) +
                           '\nUncertain jobs are NOT retried automatically. Inspect the destination before taking any manual action.', [('Refresh', 'stats'), ('Blocked Jobs', 'blocked')])
        elif action == 'blocked':
            rows = self.db.rows("SELECT source,first_id,status,error FROM jobs WHERE status IN ('failed','uncertain') ORDER BY created LIMIT 30")
            await self.say('\n'.join(f'{r["source"]} {r["first_id"]} {r["status"]}: {r["error"]}' for r in rows) or 'No blocked jobs.')
        elif action == 'logs':
            rows = self.db.rows('SELECT * FROM logs ORDER BY id DESC LIMIT 20')
            await self.say('\n'.join(time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(r['at'])) + ' ' + r['category'] for r in rows) or 'No recorded errors.')
        elif action == 'help':
            await self.say(HINDI + '\n\nConnect -> Add Sources -> Set Destinations -> Start. Only new posts after Start are accepted. Stop discards unsent queue entries. Protected content, groups, polls and forum threads are not copied. See README for supported media, uncertain sends and live acceptance tests.')
        else:
            raise UserError('This button is no longer valid. Use /start.')

    async def save_code(self, phone, result):
        if not getattr(result, 'phone_code_hash', None):
            raise UserError('Telegram requires an unsupported login step (for example payment/email). Cancel; use an official client to resolve it.')
        p = {'phone': phone, 'code_hash': result.phone_code_hash,
             'session': self.client.session.save(), 'expires': time.time()+self.auth_ttl,
             'resend_at': time.time()+max(60, getattr(result, 'timeout', None) or 60),
             'can_resend': bool(getattr(result, 'next_type', None))}
        self.db.set('pending', p)
        self.set_flow('otp')
        await self.say('Telegram ने login code request स्वीकार किया। प्राप्त code भेजें। Code/password को log या persist नहीं किया जाएगा। '
                       'Telegram may invalidate codes sent in Telegram chats; if this happens, cancel rather than repeatedly retrying.',
                       [('Resend When Allowed', 'resend'), ('Cancel Setup', 'home')])

    async def input(self, text):
        stage = self.flow.get('stage')
        if not stage:
            return await self.home()
        if self.flow.get('expires', 0) <= time.time():
            await self.cancel_auth()
            raise UserError('Input step expired. Start the workflow again.')
        if stage == 'api_id':
            if not re.fullmatch(r'[1-9]\d{3,9}', text):
                raise UserError('API ID must be a positive number, 4-10 digits.')
            self.set_flow('api_hash', api_id=int(text))
            await self.say('अब 32-character hexadecimal API HASH भेजें। इसे echo या log नहीं किया जाएगा।')
        elif stage == 'api_hash':
            if not re.fullmatch(r'[0-9a-fA-F]{32}', text):
                raise UserError('API HASH must contain exactly 32 hexadecimal characters.')
            self.db.set('credentials', {'id': self.flow['api_id'], 'hash': text})
            self.set_flow('confirm')
            await self.say('API ID: Configured\nAPI HASH: Saved encrypted\nFormat checks do not verify validity; Telegram login does.', [('Continue to Phone Login', 'continue'), ('Cancel Setup', 'home')])
        elif stage == 'confirm':
            await self.say('Use Continue to Phone Login.', [('Continue', 'continue')])
        elif stage == 'phone':
            if self.me:
                raise UserError('Account is already connected.')
            if not re.fullmatch(r'\+[1-9]\d{6,14}', text):
                raise UserError('Use international format: + followed by 7-15 digits.')
            next_request = self.db.get('next_code_request', 0)
            if time.time() < next_request:
                raise UserError('Login-code requests are cooling down. Wait before trying again.')
            self.db.set('next_code_request', time.time() + 60)
            if self.client:
                await self.client.disconnect()
            self.client = await self.new_client()
            result = await self.client.send_code_request(text)
            await self.save_code(text, result)
        elif stage in ('otp', 'password'):
            p = await self.pending_client()
            try:
                if stage == 'otp':
                    if not re.fullmatch(r'\d{4,8}', text):
                        raise UserError('Enter the numeric Telegram verification code.')
                    await self.client.sign_in(phone=p['phone'], code=text, phone_code_hash=p['code_hash'])
                else:
                    await self.client.sign_in(password=text)
            except errors.SessionPasswordNeededError:
                p['session'] = self.client.session.save()
                self.db.set('pending', p)
                self.set_flow('password')
                return await self.say('Telegram ने two-step verification मांगा है। अपने Telegram account का 2FA password भेजें। यह store नहीं होगा।')
            except errors.PhoneCodeInvalidError:
                raise UserError('Incorrect code. Check the code for this pending attempt, or cancel.')
            except errors.PhoneCodeExpiredError:
                await self.cancel_auth()
                raise UserError('Code expired or Telegram invalidated it. Login cancelled. Do not repeatedly resend codes in chat.')
            except errors.PasswordHashInvalidError:
                raise UserError('Incorrect two-step verification password. Try again or cancel.')
            finally:
                text = None
            await self.finish_auth()
        elif stage in ('search', 'add'):
            entity = await self.resolve(text, join=stage == 'add')
            if stage == 'search':
                self.set_flow()
                await self.say(f'{entity.title}\nUsername: {entity.username or "private"}\nID: {utils.get_peer_id(entity)}\n'
                               f'Membership: {"not joined" if entity.left else "member"}\nProtected: {bool(entity.noforwards)}', [('Join / Add Source', 'add')])
            else:
                sid = self.register_source(entity)
                self.set_flow()
                await self.source_detail(sid)
        elif stage == 'destination':
            entity = await self.resolve(text)
            dest = self.peer_record(entity)
            mode = self.settings['mode']
            await self.check_destination(dest, mode)
            if self.db.rows('SELECT 1 FROM sources WHERE id=?', (dest['chat'],)):
                raise UserError('A source cannot also be a destination; this would create a copy loop.')
            sid = self.flow.get('source')
            async with self.publish_lock:
                if sid:
                    row = self.db.rows('SELECT enabled FROM sources WHERE id=?', (sid,))
                    if not row or row[0]['enabled']:
                        raise UserError('Stop the source before changing its destination.')
                    self.db.execute('UPDATE sources SET dest=?,mode=? WHERE id=?', (self.db.seal(dest), mode, sid))
                else:
                    self.settings['destination'] = dest
                    self.save_settings()
                    self.db.execute('UPDATE sources SET dest=?,mode=? WHERE dest IS NULL', (self.db.seal(dest), mode))
            self.set_flow()
            await self.say('Destination permissions verified and mapping saved.')
        elif stage == 'rule':
            pair = text.split()
            if len(pair) != 2 or not all(re.fullmatch(r'@[A-Za-z0-9_]{5,32}', x) for x in pair):
                raise UserError('Use two usernames: @original @replacement')
            if len(self.settings['rules']) >= 50 and pair[0].lower() not in self.settings['rules']:
                raise UserError('Maximum 50 replacement rules.')
            self.settings['rules'][pair[0].lower()] = pair[1]
            self.save_settings()
            self.set_flow()
            await self.settings_menu()
        elif stage == 'domains':
            domains = [] if text == '-' else [x.strip().lower().rstrip('.') for x in text.split(',')]
            if len(domains) > 100 or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\.[a-z]{2,63}', d) for d in domains):
                raise UserError('Use up to 100 hostnames, without paths or protocols.')
            self.settings['domains'] = domains
            self.save_settings()
            self.set_flow()
            await self.settings_menu()
        elif stage == 'duration':
            if not text.isdigit() or int(text) > 31536000:
                raise UserError('Enter 0-31536000 seconds.')
            self.settings['duration'] = int(text)
            self.save_settings()
            self.set_flow()
            await self.settings_menu()
        elif stage == 'test':
            result, _ = transform(text, [], self.settings)
            self.set_flow()
            await self.say('Preview (not published):\n' + (result or '[empty]'))

    async def disconnect(self, revoke=False):
        await self.stop()
        if revoke:
            if not self.client:
                saved = self.db.get('session')
                if saved:
                    self.client = await self.new_client(saved)
            if self.client:
                result = await self.client.log_out()
                if not result:
                    raise UserError('Remote revocation was not confirmed. Stored session retained. Retry or disconnect locally.')
        if self.client:
            await self.client.disconnect()
        self.client, self.me = None, None
        with self.db.transaction():
            for key in ('session', 'account', 'pending', 'flow', 'joins'):
                self.db.delete(key)
            for table in ('sources', 'jobs', 'mappings'):
                self.db.execute('DELETE FROM ' + table)
        self.flow = {}
        self.settings['destination'] = None
        self.save_settings()
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        await self.say('Stored session and account-specific configuration/history deleted. API credentials and transformation settings retained. ' +
                       ('Telegram session revoked.' if revoke else 'Remote Telegram session was NOT revoked. Use Telegram Settings > Devices to terminate it if desired.') +
                       ' Backups may retain encrypted data until their retention period expires.')

    async def on_message(self, event):
        if not event.message.grouped_id:
            await self.enqueue([event.message])

    async def on_album(self, event):
        await self.enqueue(sorted(event.messages, key=lambda m: m.id))

    async def enqueue(self, messages):
        if self.closing.is_set() or not messages:
            return
        sid = messages[0].chat_id
        rows = self.db.rows('SELECT * FROM sources WHERE id=? AND enabled=1', (sid,))
        if not rows:
            return
        row, now = rows[0], time.time()
        if row['until'] and row['until'] <= now:
            return
        if messages[0].date.timestamp() < row['since']:
            return
        if any(getattr(m, 'action', None) or getattr(m, 'noforwards', False) for m in messages):
            self.db.log('SkippedServiceOrProtectedMessage')
            return
        count = self.db.rows("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','sending')")[0][0]
        if count >= self.pending_limit:
            self.db.execute("UPDATE sources SET enabled=0,status='Queue capacity reached' WHERE id=?", (sid,))
            await self.notice('capacity', 'Queue capacity reached. Source paused; review capacity before restarting.')
            return
        ids = [m.id for m in messages]
        if min(ids) <= row['last']:
            self.db.log('IgnoredLateOrAlreadyProcessedEvent')
            return
        key = 'a:' + str(messages[0].grouped_id) if messages[0].grouped_id else 'm:' + str(ids[0])
        payload = {'ids': ids, 'peer': self.db.open(row['peer']), 'dest': self.db.open(row['dest']),
                   'mode': row['mode'], 'settings': copy.deepcopy(self.settings)}
        self.db.execute('INSERT OR IGNORE INTO jobs(source,key,first_id,payload,due,created) VALUES(?,?,?,?,?,?)',
                        (sid, key, min(ids), self.db.seal(payload), now + 2, now))
        self.wake.set()

    async def fetch_job(self, row):
        p = self.db.open(row['payload'])
        source = await self.resolve_peer(self.peer(p['peer']))
        if source.left or source.noforwards:
            raise UserError('Source is inaccessible or now protects content.')
        messages = await self.client.get_messages(source, ids=p['ids'])
        if len(messages) != len(p['ids']) or any(m is None or isinstance(m, types.MessageEmpty) for m in messages):
            raise UserError('A queued source message was deleted or is inaccessible.')
        messages = sorted(messages, key=lambda m: m.id)
        if any(getattr(m, 'noforwards', False) for m in messages):
            raise UserError('Protected message cannot be copied.')
        await self.check_destination(p['dest'], p['mode'])
        content = [transform(m.message, m.entities, p['settings']) for m in messages]
        if not any(text or m.photo or m.document for m, (text, _) in zip(messages, content)):
            raise UserError('Empty or unsupported content after filtering.')
        for m in messages:
            if m.media and not (m.photo or m.document or isinstance(m.media, types.MessageMediaWebPage)):
                raise UserError('Unsupported media (poll/contact/location/paid content/etc.); job not published.')
        reply = None
        if messages[0].reply_to_msg_id:
            found = self.db.rows('SELECT sent FROM mappings WHERE source=? AND msg=? AND dest=? AND mode=?',
                                 (row['source'], messages[0].reply_to_msg_id, p['dest']['chat'], p['mode']))
            if found:
                reply = found[0]['sent']
        return p, messages, content, reply

    async def prepare_bot_media(self, messages, content, reply, dest):
        files, media = {}, []
        total_bytes = 0
        for i, (m, (text, entities)) in enumerate(zip(messages, content)):
            if not (m.photo or m.document):
                if len(messages) > 1:
                    raise UserError('Mixed text/media album cannot be published.')
                if len(add_surrogate(text)) > 4096:
                    raise UserError('Transformed text exceeds Bot API length limit.')
                data = {'chat_id': dest['chat'], 'text': text, 'entities': bot_entities(entities),
                        'link_preview_options': {'is_disabled': True}}
                if reply:
                    data['reply_parameters'] = {'message_id': reply, 'allow_sending_without_reply': True}
                return 'sendMessage', data, {}
            if len(add_surrogate(text)) > 1024:
                raise UserError('Transformed caption exceeds Bot API length limit.')
            kind = 'photo' if m.photo else 'document'
            if m.document:
                attrs = m.document.attributes
                if any(isinstance(a, types.DocumentAttributeSticker) for a in attrs):
                    raise UserError('Sticker publishing in Bot API mode is unsupported; use account mode.')
                video = next((a for a in attrs if isinstance(a, types.DocumentAttributeVideo)), None)
                audio = next((a for a in attrs if isinstance(a, types.DocumentAttributeAudio)), None)
                if video and video.round_message:
                    raise UserError('Video notes in Bot API mode are unsupported; use account mode.')
                if any(isinstance(a, types.DocumentAttributeAnimated) for a in attrs):
                    kind = 'animation'
                elif video:
                    kind = 'video'
                elif audio:
                    kind = 'voice' if audio.voice else 'audio'
            limit = min(self.max_media, (9 if kind == 'photo' else 49) * 1024 * 1024)
            if m.file and m.file.size and m.file.size > limit and m.media.document.size > limit:
                raise UserError('File exceeds the cloud Bot API upload limit. Switch this source to account mode to transfer larger files.')
            if m.file and m.file.size and m.file.size > limit:
                self.db.log('OversizedDownloadedForBotUpload')
            # Spool only into an owner-only temporary directory; no payload stored in DB.
            with tempfile.TemporaryDirectory(prefix='media-', dir=self.data_dir) as temp:
                async def progress(current, total):
                    if current > limit or (total and total > limit):
                        raise UserError('Download exceeds permitted size.')
                path = await self.client.download_media(m, file=str(Path(temp) / ('item' + (m.file.ext or '.bin'))), progress_callback=progress)
                if not path:
                    raise UserError('Media download did not return a file.')
                data_bytes = Path(path).read_bytes()
                if len(data_bytes) > limit:
                    raise UserError('Media exceeds permitted size.')
            total_bytes += len(data_bytes)
            if total_bytes > self.max_media:
                raise UserError('Combined album exceeds configured memory/upload budget. Use account mode.')
            name = 'file' + str(i)
            filename = Path(m.file.name or ('media' + (m.file.ext or '.bin'))).name
            files[name] = (filename, data_bytes)
            media.append({'type': kind, 'media': 'attach://' + name, 'caption': text,
                          'caption_entities': bot_entities(entities)})
        data = {'chat_id': dest['chat']}
        if reply:
            data['reply_parameters'] = {'message_id': reply, 'allow_sending_without_reply': True}
        if len(media) > 1:
            if not 2 <= len(media) <= 10:
                raise UserError('Bot API albums require 2-10 items.')
            kinds = {m['type'] for m in media}
            if not (kinds <= {'photo', 'video'} or kinds == {'audio'} or kinds == {'document'}):
                raise UserError('This album combines media types that Bot API does not allow.')
            data['media'] = media
            return 'sendMediaGroup', data, files
        item = media[0]
        kind = item.pop('type')
        data.update({kind: item.pop('media'), **item})
        return 'send' + kind.capitalize(), data, files

    async def publish(self, row):
        # Preflight happens before marking a job as sending: failures here are safe to retry.
        p, messages, content, reply = await self.fetch_job(row)
        prepared = None
        if p['mode'] == 'bot':
            prepared = await self.prepare_bot_media(messages, content, reply, p['dest'])
        else:
            for m, (text, _) in zip(messages, content):
                limit = 1024 if m.photo or m.document else 4096
                if len(add_surrogate(text)) > limit:
                    raise UserError('Transformed content exceeds the configured conservative Telegram length limit.')
        self.db.execute("UPDATE jobs SET status='sending' WHERE source=? AND key=?", (row['source'], row['key']))
        if prepared:
            method, data, files = prepared
            result = await self.bot.call(method, data, files)
            sent = result if isinstance(result, list) else [result]
            sent_ids = [m['message_id'] for m in sent]
        else:
            dest = self.peer(p['dest'])
            if len(messages) > 1:
                if not all(m.photo or m.document for m in messages):
                    raise UserError('Album contains unsupported media.')
                result = await self.client.send_file(dest, [m.media for m in messages],
                            caption=[x[0] for x in content], formatting_entities=[x[1] for x in content],
                            reply_to=reply)
            elif messages[0].photo or messages[0].document:
                result = await self.client.send_file(dest, messages[0].media, caption=content[0][0],
                                                    formatting_entities=content[0][1], reply_to=reply)
            else:
                result = await self.client.send_message(dest, content[0][0], formatting_entities=content[0][1],
                                                       reply_to=reply, link_preview=False)
            sent_ids = [m.id for m in (result if isinstance(result, list) else [result])]
        if len(sent_ids) != len(messages):
            raise RuntimeError('PublicationResultCountMismatch')
        with self.db.transaction():
            for original, sent_id in zip(messages, sent_ids):
                self.db.execute('INSERT OR REPLACE INTO mappings VALUES(?,?,?,?,?)',
                                (row['source'], original.id, p['dest']['chat'], p['mode'], sent_id))
            self.db.execute("UPDATE jobs SET status='sent',error=NULL WHERE source=? AND key=?", (row['source'], row['key']))
            self.db.execute('UPDATE sources SET last=MAX(last,?) WHERE id=?', (max(p['ids']), row['source']))
        if p['settings']['notify_success']:
            await self.notice('success', 'A new source post was published successfully.', interval=5)

    async def worker(self):
        while not self.closing.is_set():
            try:
                async with self.publish_lock:
                    row = None
                    if self.me and self.client.is_connected():
                        rows = self.db.rows('''SELECT j.* FROM jobs j JOIN sources s ON s.id=j.source
                          WHERE j.status='queued' AND s.enabled=1 AND j.due<=?
                          AND (s.until=0 OR s.until>?)
                          AND NOT EXISTS(SELECT 1 FROM jobs earlier WHERE earlier.source=j.source
                            AND earlier.first_id<j.first_id AND earlier.status IN ('queued','sending','uncertain','failed'))
                          ORDER BY j.created,j.first_id LIMIT 1''', (time.time(), time.time()))
                        row = rows[0] if rows else None
                    if row:
                        try:
                            await self.publish(row)
                        except (errors.FloodWaitError, BotError) as exc:
                            wait = exc.seconds if isinstance(exc, errors.FloodWaitError) else exc.retry_after
                            if wait:
                                self.retry(row, max(wait, 1))
                                await self.notice('rate', 'Telegram rate limit: queued work is waiting for the required cooldown.')
                            elif isinstance(exc, BotError) and exc.code >= 500:
                                self.fail(row, exc, uncertain=True)
                            else:
                                self.fail(row, exc)
                        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError, OSError) as exc:
                            state = self.db.rows('SELECT status FROM jobs WHERE source=? AND key=?', (row['source'], row['key']))[0][0]
                            if state == 'sending':
                                self.fail(row, exc, uncertain=True)
                            else:
                                self.retry(row, min(300, 2 ** min(row['attempt'] + 1, 8)))
                        except errors.ServerError as exc:
                            # A server error after a send may have an unknown outcome.
                            state = self.db.rows('SELECT status FROM jobs WHERE source=? AND key=?', (row['source'], row['key']))[0][0]
                            if state == 'sending':
                                self.fail(row, exc, uncertain=True)
                            else:
                                self.retry(row, 30)
                        except (errors.RPCError, UserError, ValueError, RuntimeError) as exc:
                            state = self.db.rows('SELECT status FROM jobs WHERE source=? AND key=?', (row['source'], row['key']))[0][0]
                            self.fail(row, exc, uncertain=state == 'sending' and not isinstance(exc, UserError))
                        if self.db.rows("SELECT 1 FROM jobs WHERE source=? AND key=? AND status IN ('failed','uncertain')", (row['source'], row['key'])):
                            await self.notice('publish:' + str(row['source']),
                                f'Publishing paused for source {row["source"]}. Check Error Logs and Statistics. Failed/uncertain jobs block later posts to preserve order. Use Stop, inspect the destination, then acknowledge the blocked job with /skip SOURCE_ID MESSAGE_ID.')
                if row:
                    continue
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass
            except sqlite3.Error:
                self.ready = False
                LOG.critical('FatalDatabaseError')
                self.closing.set()
                raise

    def retry(self, row, delay):
        if row['attempt'] >= 8:
            return self.fail(row, UserError('Retry limit reached'))
        self.db.execute("UPDATE jobs SET status='queued',attempt=attempt+1,due=?,error='TemporaryFailure' WHERE source=? AND key=?",
                        (time.time()+delay, row['source'], row['key']))

    def fail(self, row, exc, uncertain=False):
        label = type(exc).__name__
        if isinstance(exc, UserError):
            label += ':' + str(exc)  # UserError messages never contain credentials.
        self.db.execute('UPDATE jobs SET status=?,error=? WHERE source=? AND key=?',
                        ('uncertain' if uncertain else 'failed', label, row['source'], row['key']))
        self.db.execute('UPDATE sources SET enabled=0,status=? WHERE id=?',
                        ('Uncertain publication' if uncertain else 'Publishing failure', row['source']))
        self.db.log(label)

    async def maintenance(self):
        while not self.closing.is_set():
            await asyncio.sleep(15)
            p = self.db.get('pending')
            if p and p['expires'] <= time.time():
                async with self.admin_lock:
                    current = self.db.get('pending')
                    if current and current['expires'] <= time.time():
                        await self.cancel_auth()
                await self.notice('expired', 'Pending login expired and encrypted temporary authentication data was deleted.')
            for row in self.db.rows('SELECT id FROM sources WHERE enabled=1 AND until>0 AND until<=?', (time.time(),)):
                await self.stop(row['id'])
                await self.notice('duration:' + str(row['id']), f'Monitoring duration expired for {row["id"]}.')
            if self.me and time.time() >= self.next_checks:
                self.next_checks = time.time() + 300
                try:
                    if not self.client.is_connected():
                        await self.notice('connection', 'Telegram transport is reconnecting. Queued work is retained.')
                        await self.client.connect()
                        await self.notice('reconnected', 'Telegram transport reconnected; monitoring permissions are being rechecked.')
                    if not await self.client.is_user_authorized():
                        raise errors.AuthKeyUnregisteredError(request=None)
                    joins = self.db.get('joins', {})
                    for token, pending in list(joins.items()):
                        try:
                            invite = await self.client(functions.messages.CheckChatInviteRequest(token))
                            if isinstance(invite, types.ChatInviteAlready):
                                if isinstance(invite.chat, types.Channel) and invite.chat.broadcast:
                                    try:
                                        new_sid = self.register_source(invite.chat)
                                    except UserError as exc:
                                        self.db.log('PendingJoinCannotRegister')
                                        await self.notice('join_review', 'A tracked channel approved membership but could not be registered: ' + str(exc) + ' Check it with Add Source.')
                                    else:
                                        await self.notice('join:' + str(new_sid), f'Membership approved. Source {new_sid} was added OFF. Configure its destination, then start copying.')
                                joins.pop(token, None)
                            elif time.time() - pending['requested'] > 604800:
                                joins.pop(token, None)
                                await self.notice('join_timeout', 'A pending invitation was not confirmed within 7 days. Telegram did not confirm whether it was approved or rejected. Recheck it with Add Source.')
                        except (errors.InviteHashExpiredError, errors.InviteHashInvalidError, errors.ChannelPrivateError):
                            joins.pop(token, None)
                            await self.notice('invite_invalid', 'A tracked invitation expired or was revoked. No source was enabled.')
                        except errors.FloodWaitError as exc:
                            await asyncio.sleep(exc.seconds)
                    self.db.set('joins', joins)
                    for row in self.db.rows('SELECT * FROM sources WHERE enabled=1'):
                        try:
                            await self.check_source(row)
                        except (UserError, errors.ChannelPrivateError):
                            await self.stop(row['id'])
                            self.db.execute("UPDATE sources SET status='Access lost / protected' WHERE id=?", (row['id'],))
                            await self.notice('access:' + str(row['id']), f'Source access lost or content protected: {row["id"]}. Monitoring paused.')
                except (errors.AuthKeyUnregisteredError, errors.SessionRevokedError, errors.UserDeactivatedError):
                    await self.stop()
                    self.me = None
                    await self.notice('expired_session', 'Telegram session is no longer authorized. Monitoring stopped; reconnect required.')
                except errors.FloodWaitError as exc:
                    await self.notice('maintenance_rate', 'Permission checks are rate-limited; retrying after Telegram cooldown.')
                    await asyncio.sleep(exc.seconds)
                except (errors.RPCError, ConnectionError, OSError, asyncio.TimeoutError) as exc:
                    self.db.log('ConnectionCheck:' + type(exc).__name__)
            # Keep processed history for deduplication; no automatic destructive pruning.

    async def handle_update(self, update):
        callback = update.get('callback_query')
        message = callback.get('message', {}) if callback else update.get('message', {})
        sender = callback.get('from', {}) if callback else message.get('from', {})
        if sender.get('id') != self.owner or message.get('chat', {}).get('id') != self.owner or message.get('chat', {}).get('type') != 'private':
            if callback:
                with contextlib.suppress(BotError):
                    await self.bot.call('answerCallbackQuery', {'callback_query_id': callback['id'], 'text': 'Not authorized.'})
            return
        try:
            if callback:
                await self.bot.call('answerCallbackQuery', {'callback_query_id': callback['id']})
                await self.callback(callback.get('data', ''))
            else:
                text = message.pop('text', '')
                sensitive = self.flow.get('stage') in ('api_id', 'api_hash', 'phone', 'otp', 'password')
                if sensitive:
                    try:
                        await self.bot.call('deleteMessage', {'chat_id': self.owner, 'message_id': message['message_id']})
                    except (BotError, aiohttp.ClientError, asyncio.TimeoutError):
                        await self.notice('delete', 'Could not delete a sensitive input message. Delete it manually from this chat.')
                try:
                    if text == '/start':
                        await self.home()
                    elif text == '/cancel':
                        await self.cancel_auth()
                        await self.home()
                    elif text.startswith('/skip '):
                        match = re.fullmatch(r'/skip (-100\d+) (\d+)', text)
                        if not match:
                            raise UserError('Use /skip SOURCE_ID MESSAGE_ID after inspecting the destination.')
                        sid, mid = map(int, match.groups())
                        async with self.publish_lock:
                            source = self.db.rows('SELECT enabled FROM sources WHERE id=?', (sid,))
                            if not source or source[0]['enabled']:
                                raise UserError('Stop the source before acknowledging a blocked job.')
                            cursor = self.db.execute("UPDATE jobs SET status='skipped' WHERE source=? AND first_id=? AND status IN ('failed','uncertain')", (sid, mid))
                        await self.say(f'Acknowledged {cursor.rowcount} blocked job(s). No publication was retried. Start the source to resume.')
                    elif text == '/blocked':
                        rows = self.db.rows("SELECT source,first_id,status,error FROM jobs WHERE status IN ('failed','uncertain') ORDER BY created LIMIT 30")
                        await self.say('\n'.join(f'{r["source"]} {r["first_id"]} {r["status"]}: {r["error"]}' for r in rows) or 'No blocked jobs.')
                    else:
                        await self.input(text if self.flow.get('stage') == 'password' else text.strip())
                finally:
                    text = None
        except UserError as exc:
            await self.say(str(exc))
        except errors.FloodWaitError as exc:
            self.db.set('next_code_request', time.time() + exc.seconds)
            self.db.log('AuthenticationOrAdminFloodWait')
            await self.say(f'Telegram rate limit. Wait at least {exc.seconds} seconds before retrying this action.')
        except errors.RPCError as exc:
            self.db.log('Admin:' + type(exc).__name__)
            hints = {'ApiIdInvalidError': 'API credentials were rejected. Cancel and reset credentials.',
                     'PhoneNumberInvalidError': 'Invalid phone number.',
                     'PhoneNumberBannedError': 'Telegram has restricted this phone number.',
                     'InviteHashExpiredError': 'Invitation expired or was revoked.',
                     'InviteHashInvalidError': 'Invalid invitation link.',
                     'InviteRequestSentError': 'Join Request Pending. Recheck after administrator approval.',
                     'ChannelsTooMuchError': 'Telegram channel membership limit reached.',
                     'ChannelPrivateError': 'Channel is private or inaccessible.',
                     'UsernameNotOccupiedError': 'No channel has that username.',
                     'UsernameInvalidError': 'Invalid username.'}
            await self.say(hints.get(type(exc).__name__, 'Telegram rejected the operation: ' + type(exc).__name__ + '. Check Error Logs; secrets are not logged.'))
        except BotError as exc:
            self.db.log('AdminBotAPI:' + str(exc.code))
            await self.notice('bot_error', 'Bot API rejected the operation. Verify bot membership and destination posting permissions.')
        except (ValueError, TypeError) as exc:
            self.db.log('InvalidAdminInput:' + type(exc).__name__)
            await self.say('Input could not be resolved. Use the exact format shown in the prompt.')

    async def poll(self):
        offset = self.db.get('offset', 0)
        failures = 0
        while not self.closing.is_set():
            try:
                updates = await self.bot.call('getUpdates', {'offset': offset, 'timeout': 25,
                                                            'allowed_updates': ['message', 'callback_query']})
                self.last_poll = time.time()
                for update in updates:
                    # At-most-once admin dispatch avoids replaying OTPs or destructive actions after crashes.
                    offset = update['update_id'] + 1
                    self.db.set('offset', offset)
                    try:
                        async with self.admin_lock:
                            await self.handle_update(update)
                    except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as exc:
                        self.db.log('AdminNetwork:' + type(exc).__name__)
                        # Never replay a sensitive authentication operation after an unknown outcome.
                failures = 0
            except BotError as exc:
                if exc.code in (401, 409):
                    self.db.log('FatalBotAuthorizationOrDuplicatePoller')
                    self.closing.set()
                    raise
                await asyncio.sleep(max(exc.retry_after, min(60, 2 ** min(failures, 6))))
                failures += 1
            except (aiohttp.ClientError, asyncio.TimeoutError):
                failures += 1
                await asyncio.sleep(min(60, 2 ** min(failures, 6)))

    async def health(self, request):
        healthy = self.ready and not self.closing.is_set() and time.time()-self.last_poll < 120
        try:
            self.db.execute('SELECT 1')
        except sqlite3.Error:
            healthy = False
        return web.json_response({'status': 'ok' if healthy else 'unavailable'}, status=200 if healthy else 503)

    async def terminal_login(self):
        """Secure local/SSH fallback: login secrets never pass through bot messages."""
        import getpass
        if self.db.get('session'):
            raise UserError('An active session is stored. Disconnect it in the dashboard first.')
        await self.cancel_auth()
        credentials = self.db.get('credentials')
        if not credentials:
            api_id = input('API ID from my.telegram.org: ').strip()
            api_hash = getpass.getpass('API HASH (hidden): ').strip()
            if not re.fullmatch(r'[1-9]\d{3,9}', api_id) or not re.fullmatch(r'[0-9a-fA-F]{32}', api_hash):
                raise UserError('Invalid API credential format.')
            self.db.set('credentials', {'id': int(api_id), 'hash': api_hash})
            api_hash = None
        phone = input('Your phone number with country code: ').strip()
        if not re.fullmatch(r'\+[1-9]\d{6,14}', phone):
            raise UserError('Invalid international phone number.')
        self.client = await self.new_client()
        try:
            result = await self.client.send_code_request(phone)
            if not getattr(result, 'phone_code_hash', None):
                raise UserError('Telegram requires an unsupported login step. Resolve it with an official client.')
            code = await asyncio.to_thread(getpass.getpass, 'Telegram login code (hidden): ')
            try:
                await self.client.sign_in(phone=phone, code=code, phone_code_hash=result.phone_code_hash)
            except errors.SessionPasswordNeededError:
                password = await asyncio.to_thread(getpass.getpass, 'Telegram two-step password (hidden): ')
                try:
                    await self.client.sign_in(password=password)
                finally:
                    password = None
            finally:
                code = None
            try:
                await self.finish_auth()
            except BotError:
                if not self.me:
                    raise
                print('Session verified and encrypted. Start a private conversation with your bot for notifications.')
            print('Login complete. Start the normal service with: python bot.py')
        finally:
            if self.client:
                await self.client.disconnect()

    async def run(self):
        me = await self.bot.call('getMe')
        self.bot_id = me['id']
        webhook = await self.bot.call('getWebhookInfo')
        if webhook.get('url'):
            raise RuntimeError('ExistingWebhookMustBeRemovedBeforePolling')
        await self.resume()
        if self.db.rows("SELECT 1 FROM jobs WHERE status='uncertain' LIMIT 1"):
            await self.notice('uncertain', 'A previous publication has an unknown result. Inspect /blocked and the destination; these jobs are not retried automatically.')
        self.ready = True
        self.tasks = [asyncio.create_task(self.poll()), asyncio.create_task(self.worker()),
                      asyncio.create_task(self.maintenance())]
        stopper = asyncio.create_task(self.closing.wait())
        try:
            done, _ = await asyncio.wait([*self.tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stopper:
                    task.result()
        finally:
            self.ready = False
            self.closing.set()
            stopper.cancel()
            # Allow a current publish to complete, bounded by deployment shutdown grace.
            try:
                await asyncio.wait_for(self.publish_lock.acquire(), timeout=20)
                self.publish_lock.release()
            except asyncio.TimeoutError:
                self.db.log('ShutdownDuringPublicationOutcomeMayBeUnknown')
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            await asyncio.gather(stopper, return_exceptions=True)
            if self.client:
                await self.client.disconnect()


class OfflineTests(unittest.TestCase):
    """No real Telegram calls. Execute with --self-test; see live checklist in README."""
    def test_utf16_replacement(self):
        text = '😀 @original end'
        ent = types.MessageEntityBold(offset=3, length=9)
        config = {**DEFAULTS, 'replace': True, 'rules': {'@original': '@different'}}
        output, entities = transform(text, [ent], config)
        self.assertEqual(output, '😀 @different end')
        self.assertEqual((entities[0].offset, entities[0].length), (3, 10))

    def test_no_partial_username_match(self):
        config = {**DEFAULTS, 'replace': True, 'rules': {'@original': '@different'}}
        self.assertEqual(transform('@original_long', [], config)[0], '@original_long')

    def test_urls_are_not_replaced(self):
        config = {**DEFAULTS, 'replace': True, 'rules': {'@original': '@different'}}
        text = 'https://example.com/@original @original'
        self.assertEqual(transform(text, [], config)[0], 'https://example.com/@original @different')

    def test_remove_link_and_rebase_entity(self):
        text = 'https://bad.com bold'
        ents = [types.MessageEntityUrl(offset=0, length=15), types.MessageEntityBold(offset=16, length=4)]
        out, ents = transform(text, ents, {**DEFAULTS, 'links': 'remove'})
        self.assertEqual(out, ' bold')
        self.assertEqual((ents[0].offset, ents[0].length), (1, 4))

    def test_domain_boundaries(self):
        config = {**DEFAULTS, 'links': 'block', 'domains': ['bad.com']}
        out, _ = transform('https://bad.com https://sub.bad.com https://notbad.com', [], config)
        self.assertEqual(out, '  https://notbad.com')

    def test_hidden_link(self):
        ents = [types.MessageEntityTextUrl(offset=0, length=5, url='https://bad.com')]
        out, ents = transform('label ok', ents, {**DEFAULTS, 'links': 'remove'})
        self.assertEqual((out, ents), (' ok', []))

    def test_allowlist(self):
        config = {**DEFAULTS, 'links': 'allow', 'domains': ['good.com']}
        self.assertEqual(transform('https://good.com https://bad.com', [], config)[0], 'https://good.com ')

    def test_bot_entity_conversion(self):
        ents = bot_entities([types.MessageEntityCustomEmoji(offset=0, length=2, document_id=123)])
        self.assertEqual(ents[0]['custom_emoji_id'], '123')

    def test_database_crypto_uniqueness_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'db.sqlite3'
            key = Fernet.generate_key()
            db = Store(path, key)
            db.set('secret', 'do-not-store-plaintext')
            self.assertEqual(db.get('secret'), 'do-not-store-plaintext')
            raw = db.rows("SELECT v FROM secrets WHERE k='secret'")[0][0]
            self.assertNotIn(b'do-not-store-plaintext', raw)
            payload = db.seal({'ids': [1]})
            for _ in range(2):
                db.execute('INSERT OR IGNORE INTO jobs(source,key,first_id,payload,status,due,created) VALUES(1,?,1,?,?,0,0)', ('m:1', payload, 'sending'))
            self.assertEqual(db.rows('SELECT COUNT(*) FROM jobs')[0][0], 1)
            db.close()
            db = Store(path, key)
            self.assertEqual(db.rows('SELECT status FROM jobs')[0][0], 'uncertain')
            db.close()

    def test_transaction_rollback(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Store(Path(tmp) / 'db', Fernet.generate_key())
            with self.assertRaises(ValueError):
                with db.transaction():
                    db.set('example', 1)
                    raise ValueError()
            self.assertIsNone(db.get('example'))
            db.close()

    def test_wrong_key(self):
        token = Fernet(Fernet.generate_key()).encrypt(b'secret')
        with self.assertRaises(InvalidToken):
            Fernet(Fernet.generate_key()).decrypt(token)

    def test_keyboard_callbacks(self):
        markup = keyboard(MENU)
        for row in markup['inline_keyboard']:
            self.assertLessEqual(len(row[0]['callback_data'].encode()), 64)

    def test_resolve_peer_and_group_boundaries(self):
        self.assertIn('resolve_peer', dir(Application))
        self.assertIsNotNone(getattr(types, 'InputPeerChannel'))

    def test_schema_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Store(Path(tmp) / 'db', Fernet.generate_key())
            self.assertEqual(db.rows('PRAGMA user_version')[0][0], 1)
            db.close()


async def main(terminal_login=False):
    load_dotenv()
    os.umask(0o077)

    # Bind Render's health port immediately: the very first thing the process
    # does, before env validation, disk access, or any Telegram call. Render's
    # deploy-time port scan only needs a listening socket; binding this early
    # means the scan succeeds even if everything below is slow or misconfigured,
    # instead of looping on "No open ports detected" while startup is still in
    # progress (or crash-looping before it ever reaches a bind).
    port = os.getenv('PORT')
    server = None
    health_ref = {'app': None}
    if port:
        async def health_handler(request):
            app = health_ref['app']
            if app is None:
                return web.json_response({'status': 'starting'}, status=503)
            return await app.health(request)
        site_app = web.Application()
        site_app.router.add_get('/healthz', health_handler)
        server = web.AppRunner(site_app, access_log=None)
        await server.setup()
        await web.TCPSite(server, '0.0.0.0', int(port)).start()
        LOG.info('Health endpoint listening on port %s', port)

    try:
        owner = int(os.environ['OWNER_ID'])
        if owner <= 0:
            raise RuntimeError('OWNER_ID must be a positive numeric Telegram user ID')
        token = os.environ['BOT_TOKEN']
        if not re.fullmatch(r'\d+:[A-Za-z0-9_-]+', token):
            raise RuntimeError('BOT_TOKEN format is invalid')
        key = os.environ['ENCRYPTION_KEY'].encode()
        Fernet(key)  # Fail closed; never silently generate/replace an existing key.
        directory = Path(os.getenv('DATA_DIR', './data')).resolve()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(directory, 0o700)
        # Linux/Render lock is held for process lifetime, before polling or migrations.
        import fcntl
        with open(directory / 'instance.lock', 'a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('Another instance holds this data directory') from None
            # A killed process may leave partial downloads; none are needed for recovery.
            import shutil
            for path in directory.glob('media-*'):
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
            db = Store(directory / 'copier.sqlite3', key)
            timeout = aiohttp.ClientTimeout(total=90, connect=20)
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    app = Application(db, BotAPI(session, token), owner, directory)
                    health_ref['app'] = app
                    loop = asyncio.get_running_loop()
                    for sig in (signal.SIGINT, signal.SIGTERM):
                        loop.add_signal_handler(sig, app.closing.set)
                    if terminal_login:
                        await app.terminal_login()
                    else:
                        await app.run()
            finally:
                db.close()
    finally:
        if server:
            await server.cleanup()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--login', action='store_true', help='Interactive terminal login; stop the normal service first')
    args = parser.parse_args()
    # Suppress third-party logs that might contain authentication payloads or tokens.
    logging.basicConfig(level=logging.CRITICAL, format='%(levelname)s %(message)s')
    LOG.setLevel(logging.INFO)
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(OfflineTests)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        sys.exit(0 if result.wasSuccessful() else 1)
    try:
        asyncio.run(main(terminal_login=args.login))
    except KeyboardInterrupt:
        pass
    except RuntimeError as exc:
        # Every RuntimeError this app raises itself uses a static, secret-free
        # message (bad OWNER_ID/BOT_TOKEN, stale instance lock, wrong key,
        # existing webhook, etc.), so it's safe and far more useful to log it.
        LOG.critical('Application stopped: %s', exc)
        sys.exit(1)
    except Exception as exc:
        # Everything else may originate from a library and could echo
        # request data (tokens, etc.) in its message, so only the type is logged.
        LOG.critical('Application stopped: %s. Check configuration and controlled error logs.', type(exc).__name__)
        sys.exit(1)
