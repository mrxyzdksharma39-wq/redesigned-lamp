"""Owner-only Telegram copier. Python 3.11+. See README before deployment."""
import asyncio
import contextlib
import getpass
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from cryptography.fernet import Fernet
from sqlalchemy import (BigInteger, Boolean, Float, Integer, String, Text,
                        UniqueConstraint, delete, select, text, update)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.exc import IntegrityError
from aiogram import Bot, Dispatcher
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, InputMediaAudio, InputMediaDocument,
                           InputMediaPhoto, InputMediaVideo, Message, MessageEntity,
                           ReplyParameters)
from aiogram.exceptions import (TelegramBadRequest, TelegramForbiddenError,
                                TelegramNetworkError, TelegramRetryAfter,
                                TelegramServerError)
from telethon import TelegramClient, events, functions, types, utils, errors
from telethon.sessions import StringSession

load_dotenv()
# Third-party exception strings can contain credentials or private channel links.
logging.disable(logging.CRITICAL)
DEFAULTS = dict(replace=True, rules=[], links="keep", domains=[], formatting=True,
                media=True, replies=True, duration=0)
SUPPORTED_ENTITIES = {
    "mention", "hashtag", "cashtag", "bot_command", "url", "email",
    "bold", "italic", "underline", "strikethrough", "spoiler", "code", "pre",
    "text_link", "text_mention", "custom_emoji", "blockquote", "expandable_blockquote",
    "phone_number"}


class Base(DeclarativeBase):
    pass


class Config(Base):
    __tablename__ = "owner_config"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="{}")


class Route(Base):
    __tablename__ = "routes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[int] = mapped_column(BigInteger)
    source_name: Mapped[str] = mapped_column(Text)
    source_ref: Mapped[str] = mapped_column(Text)
    mode: Mapped[str] = mapped_column(String(8))
    dest: Mapped[int] = mapped_column(BigInteger)
    dest_name: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    started: Mapped[float] = mapped_column(Float, default=0)
    expires: Mapped[float] = mapped_column(Float, default=0)
    settings: Mapped[str] = mapped_column(Text, default=lambda: json.dumps(DEFAULTS))
    last_error: Mapped[str] = mapped_column(Text, default="")
    last_success: Mapped[float] = mapped_column(Float, default=0)
    __table_args__ = (UniqueConstraint("source", "dest"), {"sqlite_autoincrement": True})


class Job(Base):
    __tablename__ = "publishing_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    route: Mapped[int] = mapped_column(Integer, index=True)
    source_message: Mapped[int] = mapped_column(BigInteger)
    group_key: Mapped[str] = mapped_column(String(64), default="")
    payload: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    created: Mapped[float] = mapped_column(Float)
    error: Mapped[str] = mapped_column(Text, default="")
    __table_args__ = (UniqueConstraint("route", "source_message"),)


class MessageMap(Base):
    __tablename__ = "message_mappings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    route: Mapped[int] = mapped_column(Integer)
    source_message: Mapped[int] = mapped_column(BigInteger)
    destination_message: Mapped[int] = mapped_column(BigInteger)
    __table_args__ = (UniqueConstraint("route", "source_message"),)


class UserFacingError(Exception):
    pass


def stamp():
    return time.time()


def units(s):
    return len(s.encode("utf-16-le")) // 2


def slice16(s, a, b):
    return s.encode("utf-16-le")[2*a:2*b].decode("utf-16-le")


def domain_of(url):
    value = url if "://" in url else "https://" + url
    return (urlparse(value).hostname or "").lower().rstrip(".")


def parse_reference(value):
    value = value.strip()
    if re.fullmatch(r"-100[0-9]{5,}", value):
        return "id", int(value)
    if re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,31}", value):
        return "public", value
    p = urlparse(value if "://" in value else "https://" + value)
    if p.scheme != "https" or p.hostname not in {"t.me", "telegram.me"} or p.query or p.fragment:
        raise UserFacingError("Use @Channel, https://t.me/Channel, a private invite link, or a known -100 channel ID.")
    path = p.path.strip("/")
    m = re.fullmatch(r"(?:\+|joinchat/)([A-Za-z0-9_-]+)", path)
    if m:
        return "invite", m[1]
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", path):
        return "public", "@" + path
    raise UserFacingError("Use a channel link, not a post link, /c/ link, or discussion link.")


def transform(text_value, entities, settings):
    """UTF-16 edits; preserve enclosing styles, drop invalid atomic entities."""
    original = text_value or ""
    es = [dict(e) for e in entities]
    for e in es:
        if e["type"] not in SUPPORTED_ENTITIES:
            raise UserFacingError("This post has an unsupported formatting entity.")
    edits = []
    if settings["replace"]:
        for old, new in settings["rules"]:
            pattern = r"(?<![\w@])" + re.escape(old) + r"(?![A-Za-z0-9_])"
            for m in re.finditer(pattern, original, flags=re.I):
                edits.append((units(original[:m.start()]), units(original[:m.end()]), new))
        for e in es:
            if e["type"] == "text_link":
                for old, new in settings["rules"]:
                    if re.fullmatch(r"https?://(?:t\.me|telegram\.me)/" + re.escape(old[1:]) + r"/?", e.get("url", ""), flags=re.I):
                        e["url"] = "https://t.me/" + new[1:]
                        break
    links = []
    for e in es:
        if e["type"] in {"url", "text_link"}:
            a, b = e["offset"], e["offset"] + e["length"]
            links.append((a, b, e.get("url") or slice16(original, a, b), e["type"]))
    # Detect pasted URLs even when Telegram didn't supply a URL entity.
    for m in re.finditer(r"(?i)(?:https?://|www\.|(?:t|telegram)\.me/)[^\s<>]+", original):
        a, b = units(original[:m.start()]), units(original[:m.end()])
        if not any(a < y and b > x for x, y, _, _ in links):
            links.append((a, b, m.group(), "url"))
    for a, b, url, kind in links:
        host = domain_of(url)
        blocked = settings["links"] == "remove" or (settings["links"] == "block" and any(
            host == d or host.endswith("." + d) for d in settings["domains"]))
        if blocked:
            if kind == "text_link":
                es = [e for e in es if not (e["type"] == "text_link" and e["offset"] == a and e["offset"]+e["length"] == b)]
            else:
                edits = [v for v in edits if not (v[0] < b and v[1] > a)]
                edits.append((a, b, ""))
    # Apply right to left so original offsets remain meaningful.
    for a, b, replacement in sorted(set(edits), reverse=True):
        delta = units(replacement) - (b-a)
        original = slice16(original, 0, a) + replacement + slice16(original, b, units(original))
        updated = []
        for e in es:
            e = dict(e)
            x, y = e["offset"], e["offset"] + e["length"]
            if y <= a:
                pass
            elif x >= b:
                e["offset"] += delta
            elif e["type"] in {"bold", "italic", "underline", "strikethrough", "spoiler", "code", "pre", "blockquote", "expandable_blockquote"}:
                nx = x if x <= a else a
                ny = y + delta if y >= b else a + units(replacement)
                e["offset"], e["length"] = nx, ny - nx
            elif x <= a and y >= b and e["type"] == "text_link":
                e["length"] += delta
            elif x == a and y == b and replacement and e["type"] == "mention":
                e["length"] = units(replacement)
            else:
                continue
            if e["length"] > 0:
                updated.append(e)
        es = updated
    return original, es if settings["formatting"] else []


def keyboard(rows, back=True):
    data = [[InlineKeyboardButton(text=label, callback_data=action) for label, action in row] for row in rows]
    if back:
        data.append([InlineKeyboardButton(text="🔙 Back", callback_data="home"),
                     InlineKeyboardButton(text="Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=data)


def describe_error(exc):
    if isinstance(exc, UserFacingError):
        return str(exc)
    if isinstance(exc, TelegramRetryAfter):
        return "Telegram rate limit. Wait before retrying."
    if isinstance(exc, TelegramForbiddenError):
        return "Bot access denied. Restore channel membership and administrator Post Messages permission."
    if isinstance(exc, (TelegramNetworkError, TelegramServerError, OSError, asyncio.TimeoutError)):
        return "Network/server error. Delivery may be uncertain; check destination before taking manual action."
    if isinstance(exc, TelegramBadRequest):
        return "Telegram rejected the request. Check channel access, media limits, entity support and reply availability."
    if isinstance(exc, errors.FloodWaitError):
        return "Telegram user-account rate limit. Wait and try again."
    return "Operation failed (" + type(exc).__name__ + "). Check configuration and access; private details were not logged."


class Copier:
    def __init__(self):
        self.owner = int(os.environ["OWNER_ID"])
        self.bot = Bot(os.environ["BOT_TOKEN"])
        self.dp = Dispatcher()
        url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/bot.db")
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif url.startswith("sqlite://"):
            url = url.replace("sqlite://", "sqlite+aiosqlite://", 1)
        if url.startswith("sqlite"):
            p = url.split("///", 1)[1]
            Path(p).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_async_engine(url, pool_pre_ping=True, **({"connect_args": {"timeout": 30}} if url.startswith("sqlite") else {}))
        self.db = async_sessionmaker(self.engine, expire_on_commit=False)
        self.pg = None
        self.lockfile = None
        self.user = None
        self.me = None
        self.ready_at = stamp()
        self.flows = None
        self.workers = {}
        self.wakes = {}
        self.stop_events = {}
        self.admin_lock = asyncio.Lock()
        self.config_lock = asyncio.Lock()
        self.tasks = []
        self.dp.callback_query.register(self.callback)
        self.dp.message.register(self.message)
        self.dp.channel_post.register(self.channel_post)
        self.dp.error.register(self.error_handler)

    async def error_handler(self, event):
        await self.notify("An update could not be handled: " + describe_error(event.exception), critical=True)
        return True

    async def config(self, **changes):
        async with self.config_lock:
            async with self.db() as s:
                row = await s.get(Config, self.owner)
                new = row is None
                if new:
                    row = Config(id=self.owner, value="{}")
                    s.add(row)
                value = json.loads(row.value)
                if changes or new:
                    value.update(changes)
                    row.value = json.dumps(value)
                    await s.commit()
                return value

    async def route(self, rid):
        async with self.db() as s:
            return await s.get(Route, rid)

    async def routes(self):
        async with self.db() as s:
            return list((await s.scalars(select(Route).order_by(Route.id))).all())

    async def selected(self):
        cfg = await self.config()
        r = await self.route(cfg.get("selected", 0))
        if not r:
            raise UserFacingError("Add a destination, then add a source. Select a mapping if you have several.")
        return r

    async def notify(self, value, critical=False, success=False):
        cfg = await self.config()
        if not critical and not cfg.get("notifications", True):
            return
        if success and not cfg.get("success_notifications", False):
            return
        with contextlib.suppress(Exception):
            await self.bot.send_message(self.owner, value)

    async def show(self, text_value, rows=None, back=True):
        await self.bot.send_message(self.owner, text_value[:4000], reply_markup=keyboard(rows or [], back))

    async def home(self):
        rs = await self.routes()
        active = sum(r.enabled for r in rs)
        await self.show("🤖 Telegram Auto-Copy Bot\nStatus: 🟢 Online\nMonitoring: " + str(active) + " mapping(s) ON\nStart/Stop applies to the selected mapping.", [
            [("➕ Add Your Channel", "dest"), ("📥 Add Source Channel", "source")],
            [("⚙️ Copy Settings", "settings"), ("▶️ Start Copying", "start")],
            [("⏹ Stop Copying", "stop"), ("📊 Copy Status", "status")],
            [("🗑 Remove Channel", "remove"), ("🔔 Notifications", "notifications")],
            [("❓ Help", "help"), ("Select Mapping", "mappings")]], False)

    async def prompt(self, name, text_value, **extra):
        self.flows = dict(name=name, **extra)
        await self.show(text_value)

    async def check_dest(self, cid):
        try:
            chat = await self.bot.get_chat(cid)
        except (TelegramBadRequest, TelegramForbiddenError):
            raise UserFacingError("Destination inaccessible. Confirm its username/ID, add this bot under channel Administrators, and enable Post Messages. Private invite links do not add bots.")
        if chat.type != "channel":
            raise UserFacingError("Destination must be a broadcast channel, not a group.")
        member = await self.bot.get_chat_member(chat.id, self.me.id)
        if member.status not in {"creator", "administrator"}:
            raise UserFacingError("Open destination > Manage Channel > Administrators > Add this bot. Enable Post Messages.")
        if member.status == "administrator" and not member.can_post_messages:
            raise UserFacingError("Open destination > Administrators > this bot, and enable Post Messages.")
        return chat

    async def check_source(self, r):
        if r.mode == "bot":
            chat = await self.bot.get_chat(r.source)
            member = await self.bot.get_chat_member(r.source, self.me.id)
            if member.status not in {"administrator", "creator"}:
                raise UserFacingError("Bot API source access requires this bot as a source-channel administrator. Alternatively use your authorized user connection.")
            if chat.has_protected_content:
                raise UserFacingError("Source has protected content. Copying is not permitted.")
        else:
            if not self.user:
                raise UserFacingError("User connection unavailable. Run the secure local authentication setup and restart.")
            entity = await self.user.get_entity(r.source)
            if not isinstance(entity, types.Channel) or not entity.broadcast:
                raise UserFacingError("Source must be a broadcast channel.")
            if getattr(entity, "noforwards", False):
                raise UserFacingError("Source has protected content. Copying is not permitted.")
            # Membership, not merely public history access, is needed for live events.
            await self.user(functions.channels.GetParticipantRequest(entity, "me"))
            await self.user.get_messages(entity, limit=1)

    async def resolve_source(self, value, mode, allow_join=False):
        kind, ref = parse_reference(value)
        if mode == "bot":
            if kind == "invite":
                raise UserFacingError("Bots cannot join by invite. Add this bot as source administrator and submit its -100 channel ID, or select User Account.")
            chat = await self.bot.get_chat(ref)
            r = Route(source=chat.id, mode=mode)
            if chat.type != "channel":
                raise UserFacingError("Source must be a broadcast channel.")
            await self.check_source(r)
            return chat.id, chat.title, ("@" + chat.username) if chat.username else str(chat.id)
        if not self.user:
            raise UserFacingError("Run python bot.py auth locally using your own account, then deploy the encrypted session. See README.")
        if kind == "invite":
            invitation = await self.user(functions.messages.CheckChatInviteRequest(ref))
            if isinstance(invitation, types.ChatInviteAlready):
                entity = invitation.chat
            else:
                if not getattr(invitation, "broadcast", False):
                    raise UserFacingError("This invite is not for a broadcast channel.")
                if not allow_join:
                    self.flows = dict(name="join", value=value, mode=mode)
                    await self.show("Your authorized account is not a member. Join this channel or submit its join request? No mapping is saved until membership is verified.", [[("Join / Request Access", "join")]])
                    return None
                try:
                    result = await self.user(functions.messages.ImportChatInviteRequest(ref))
                    entity = result.chats[0]
                except errors.InviteRequestSentError:
                    raise UserFacingError("Join request submitted. After approval, add this source again. Nothing has been connected yet.")
        else:
            entity = await self.user.get_entity(ref)
            if not isinstance(entity, types.Channel) or not entity.broadcast:
                raise UserFacingError("Source must be a broadcast channel, not a person or group.")
            try:
                await self.user(functions.channels.GetParticipantRequest(entity, "me"))
            except errors.UserNotParticipantError:
                if not allow_join:
                    self.flows = dict(name="join", value=value, mode=mode)
                    await self.show("Live monitoring requires your authorized account to join this public channel. Join now?", [[("Join Channel", "join")]])
                    return None
                try:
                    await self.user(functions.channels.JoinChannelRequest(entity))
                except errors.InviteRequestSentError:
                    raise UserFacingError("Join request submitted. Add the source again after approval.")
        sid = utils.get_peer_id(entity)
        # Re-resolve via cached input entity; private access hashes stay only in memory.
        await self.check_source(Route(source=sid, mode=mode))
        return sid, entity.title, ("@" + entity.username) if entity.username else str(sid)

    async def add_source(self, value, mode, allow_join=False, replacing=None):
        cfg = await self.config()
        dest = cfg.get("destination")
        if not dest:
            raise UserFacingError("Add Your Channel first, then add a source.")
        await self.check_dest(dest["id"])
        resolved = await self.resolve_source(value, mode, allow_join)
        if resolved is None:
            if replacing and self.flows:
                self.flows["replacing"] = replacing
            return
        sid, name, ref = resolved
        if sid == dest["id"]:
            raise UserFacingError("Source and destination cannot be the same channel.")
        rs = await self.routes()
        # Reject cycles including indirect ones.
        graph = {}
        for r in rs:
            graph.setdefault(r.source, []).append(r.dest)
        stack, visited = [dest["id"]], set()
        while stack:
            v = stack.pop()
            if v == sid:
                raise UserFacingError("This mapping would create a copying loop.")
            if v not in visited:
                visited.add(v)
                stack.extend(graph.get(v, []))
        async with self.db() as s:
            existing = await s.scalar(select(Route).where(Route.source == sid, Route.dest == dest["id"]))
            if existing:
                raise UserFacingError("That mapping already exists. Select it from Select Mapping.")
            r = Route(source=sid, source_name=name, source_ref=ref, mode=mode,
                      dest=dest["id"], dest_name=dest["name"])
            s.add(r)
            await s.commit()
            rid = r.id
        if replacing:
            old = await self.route(replacing)
            if old:
                await self.set_settings(rid, **json.loads(old.settings))
                self.wakes.setdefault(old.id, asyncio.Event()).set()
                task = self.workers.get(old.id)
                if task and not task.done():
                    await task
                async with self.db() as s:
                    await s.execute(delete(Route).where(Route.id == old.id))
                    await s.commit()
        self.flows = None
        await self.config(selected=rid)
        await self.show(f"Source Channel: {name}\n{ref}\nAccess: ✅ Verified ({mode})\nCopy Status: ⏸ Stopped\nDestination: {dest['name']}", [
            [("▶️ Start Copying", "start"), ("🔄 Change Source", "change_source")],
            [("🗑 Remove Source", "remove_source")]])

    async def set_settings(self, rid, **changes):
        async with self.db() as s:
            r = await s.get(Route, rid)
            if not r:
                raise UserFacingError("Mapping no longer exists.")
            cfg = json.loads(r.settings)
            cfg.update(changes)
            r.settings = json.dumps(cfg)
            await s.commit()
        return cfg

    async def settings_menu(self):
        r = await self.selected()
        await self.show(f"⚙️ Copy Settings\n{r.source_name} -> {r.dest_name}", [
            [("🔤 Username Replacement", "replacement"), ("🔗 Link Filter", "links")],
            [("✍️ Formatting", "formatting"), ("🖼 Media Settings", "media")],
            [("⏱ Copy Duration", "duration"), ("📌 Reply Mapping", "replies")]])

    async def replacement_menu(self):
        r = await self.selected()
        cfg = json.loads(r.settings)
        rules = "\n".join(a + " -> " + b for a, b in cfg["rules"]) or "No rules yet"
        await self.show("Username Replacement: " + ("ON" if cfg["replace"] else "OFF") + "\n" + rules, [
            [("Add Rule", "rule"), ("Clear Rules", "clear_rules")],
            [("✅ Enable Replacement", "replace_on"), ("⏸ Disable Replacement", "replace_off")],
            [("🧪 Test Replacement", "test")]])

    async def status(self):
        r = await self.selected()
        async with self.db() as s:
            states = (await s.execute(text("SELECT state, COUNT(*) FROM publishing_history WHERE route=:r GROUP BY state"), {"r": r.id})).all()
        counts = dict(states)
        cfg = json.loads(r.settings)
        remaining = (str(max(0, int(r.expires-stamp()))) + " seconds") if r.expires else "Until stopped"
        when = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(r.last_success)) if r.last_success else "None"
        await self.show(f"📊 Copy Status\nSource: {r.source_name} ({r.source_ref})\nDestination: {r.dest_name}\nMonitoring: {'ON' if r.enabled else 'OFF'}\nSelected Duration: {cfg['duration'] or 'Unlimited'} seconds\nTime Remaining: {remaining if r.enabled else 'Stopped'}\nPosts Successfully Published: {counts.get('sent',0)}\nFailed Posts: {counts.get('failed',0)}\nUncertain delivery: {counts.get('uncertain',0)}\nPending: {counts.get('pending',0)}\nSkipped: {counts.get('skipped',0)}\nLast Successful Post: {when}\nLast Error: {r.last_error or 'None'}", [
            [("▶️ Start", "start"), ("⏹ Stop", "stop")], [("🔄 Refresh", "status")]])

    async def start(self, rid, resume=False):
        r = await self.route(rid)
        if not r:
            raise UserFacingError("Mapping no longer exists.")
        if r.enabled and not resume:
            await self.show("Already monitoring. The saved expiry has not been extended.")
            return
        old_worker = self.workers.get(rid)
        if not r.enabled and old_worker and not old_worker.done():
            self.wakes[rid].set()
            await old_worker
        await self.check_source(r)
        await self.check_dest(r.dest)
        now = stamp()
        if resume and r.expires and r.expires <= now:
            await self.stop(rid, "Configured duration expired.")
            return
        async with self.db() as s:
            row = await s.get(Route, rid)
            row.enabled = True
            if not resume:
                row.started = now
                duration = json.loads(row.settings)["duration"]
                row.expires = now + duration if duration else 0
                await s.execute(update(Job).where(Job.route == rid, Job.state == "pending").values(state="skipped", error="Previous run stopped"))
            row.last_error = ""
            await s.commit()
        self.stop_events.setdefault(rid, asyncio.Event()).clear()
        self.ensure_worker(rid)
        self.wakes[rid].set()
        if not resume:
            await self.notify(f"🟢 Copying Started\nSource: {r.source_name}\nDestination: {r.dest_name}\nMode: New Posts Only\nDuration: {json.loads(r.settings)['duration'] or 'Until Stopped'}")

    async def stop(self, rid, reason="Copying stopped."):
        self.stop_events.setdefault(rid, asyncio.Event()).set()
        async with self.db() as s:
            r = await s.get(Route, rid)
            if not r:
                return
            r.enabled = False
            if any(term in reason.lower() for term in ("lost", "failure", "failed", "manual review")):
                r.last_error = reason
            await s.execute(update(Job).where(Job.route == rid, Job.state == "pending").values(state="skipped", error=reason))
            await s.commit()
        self.wakes.setdefault(rid, asyncio.Event()).set()
        await self.notify(reason + "\n" + r.source_name + " -> " + r.dest_name)

    async def callback(self, q: CallbackQuery):
        if q.from_user.id != self.owner or not q.message or q.message.chat.id != self.owner:
            await q.answer("Owner only", show_alert=True)
            return
        await q.answer()
        async with self.admin_lock:
            try:
                await self._callback(q.data or "")
            except Exception as exc:
                await self.show(describe_error(exc))

    async def _callback(self, action):
        if len(action) > 64:
            raise UserFacingError("Invalid button.")
        if action in {"home", "cancel"}:
            self.flows = None
            return await self.home()
        if action == "dest":
            return await self.prompt("dest", "Enter destination @username, public link, or known -100 channel ID. For private invites, the authorized account must already be a member; the bot must separately be an administrator with Post Messages.")
        if action == "source":
            return await self.show("Choose source connection.\nBot API: add the bot as source administrator.\nUser Account: your own authenticated account must legitimately be a channel member; the source owner need not add this bot.", [[("🤖 Bot API", "source_bot"), ("👤 User Account", "source_user")]])
        if action in {"source_bot", "source_user"}:
            return await self.prompt("source", "Enter source @username, channel link, invite link or known -100 channel ID.", mode=action.split("_")[1])
        if action == "join":
            f = self.flows
            if not f or f["name"] != "join":
                raise UserFacingError("This confirmation expired. Add the source again.")
            return await self.add_source(f["value"], f["mode"], True, replacing=f.get("replacing"))
        if action == "mappings":
            rs = await self.routes()
            return await self.show("Select a mapping. Each mapping has independent settings and Start/Stop.", [[(f"{'🟢' if r.enabled else '⏸'} {r.source_name[:22]} -> {r.dest_name[:22]}", f"select:{r.id}")] for r in rs])
        if re.fullmatch(r"select:[0-9]+", action):
            rid = int(action.split(":")[1])
            if not await self.route(rid):
                raise UserFacingError("Mapping no longer exists.")
            self.flows = None
            await self.config(selected=rid)
            return await self.status()
        if action == "help":
            return await self.show("1. Add Your Channel: add the bot as destination administrator with Post Messages.\n2. Add Source: Bot API needs source administrator access; User Account needs your own secure local login and legitimate membership.\n3. Select mapping, choose settings, then Start.\nOnly new posts are copied. Stop preserves configuration.\nMultiple sources: repeat Add Source; use Select Mapping to manage each. Destination changes affect only the selected stopped mapping and the default for new sources.\nOnly copy content you have permission to reproduce. Protected content is refused.\nNo passwords or verification codes are collected in this chat. Unsupported media and uncertain sends are reported, not silently retried. See README for limitations.")
        if action == "notifications":
            c = await self.config()
            return await self.show(f"Notifications: {'ON' if c.get('notifications',True) else 'OFF'}\nSuccess messages: {'ON' if c.get('success_notifications',False) else 'OFF'}\nCritical failures always notify.", [[("Toggle Notifications", "notify_toggle")], [("Toggle Successful Publishing", "success_toggle")]])
        if action in {"notify_toggle", "success_toggle"}:
            c = await self.config()
            key = "notifications" if action == "notify_toggle" else "success_notifications"
            await self.config(**{key: not c.get(key, key == "notifications")})
            return await self._callback("notifications")
        if action == "remove":
            return await self.show("Remove the selected mapping/source, or the selected destination and all mappings that use it. Publishing history remains for audit. Confirm on the next screen.", [[("Remove Source / Mapping", "remove_source")], [("Remove Destination", "remove_dest")]])
        if action == "remove_dest" and not await self.routes():
            return await self.show("Remove the saved default destination?", [[("Confirm removal", "remove_default")]])
        if action == "remove_default":
            if await self.routes():
                raise UserFacingError("Mappings exist. Select one and remove its destination instead.")
            await self.config(destination=None, selected=0)
            return await self.home()
        r = await self.selected()
        cfg = json.loads(r.settings)
        if action == "start":
            await self.start(r.id)
            return await self.status()
        if action == "stop":
            await self.stop(r.id)
            return await self.status()
        if action == "status":
            return await self.status()
        if action == "settings":
            return await self.settings_menu()
        if action == "change_source":
            if r.enabled:
                raise UserFacingError("Stop the mapping before changing its source.")
            return await self.prompt("change_source", "Enter replacement source channel reference. Existing source connection type will be used.", rid=r.id)
        if action in {"remove_source", "remove_dest"}:
            return await self.show("Confirm removal? This cannot be undone.", [[("Confirm removal", f"confirm:{action}:{r.id}")]])
        if re.fullmatch(r"confirm:remove_(?:source|dest):[0-9]+", action):
            _, kind, rid = action.split(":")
            target = await self.route(int(rid))
            if not target:
                raise UserFacingError("Mapping no longer exists.")
            targets = [x for x in await self.routes() if x.dest == target.dest] if kind == "remove_dest" else [target]
            for item in targets:
                await self.stop(item.id, "Channel mapping removed.")
                task = self.workers.get(item.id)
                if task and not task.done():
                    # An already dispatched Telegram call may complete; wait before deleting.
                    await task
                async with self.db() as s:
                    await s.execute(delete(Route).where(Route.id == item.id))
                    await s.commit()
            c = await self.config()
            if kind == "remove_dest" and (c.get("destination") or {}).get("id") == target.dest:
                await self.config(destination=None, selected=0)
            else:
                await self.config(selected=0)
            return await self.home()
        if action == "replacement":
            return await self.replacement_menu()
        if action == "rule":
            return await self.prompt("rule_old", "Original Username: enter @OldSupport", rid=r.id)
        if action in {"replace_on", "replace_off", "clear_rules"}:
            await self.set_settings(r.id, **({"rules": []} if action == "clear_rules" else {"replace": action == "replace_on"}))
            return await self.replacement_menu()
        if action == "test":
            return await self.prompt("test", "Send sample text to preview the current rules. No channel post is published.", rid=r.id)
        if action == "links":
            return await self.show(f"Link Filter: {cfg['links']}\nKeep All: leave links unchanged.\nRemove URLs: remove visible URL text; unlink hidden links while keeping their label.\nBlock Domains: same removal, limited to selected domains and their subdomains. Formatting is rebuilt with UTF-16 offsets.", [[("✅ Keep All Links", "link_keep")], [("🚫 Remove URLs", "link_remove")], [("🎯 Block Selected Domains", "domains")]])
        if action in {"link_keep", "link_remove"}:
            await self.set_settings(r.id, links=action.split("_")[1])
            return await self._callback("links")
        if action == "domains":
            return await self.prompt("domains", "Enter domains separated by commas, e.g. example.com, ads.example.org. These domains AND subdomains will be blocked. Hidden links lose their target, not their label.", rid=r.id)
        if action in {"formatting", "media", "replies"}:
            explanation = {"formatting": "Preserve supported message entities. OFF explicitly sends plain text/captions.", "media": "ON copies supported photos, videos, documents, audio and albums. OFF skips whole media posts, including their captions.", "replies": "Reply only when the original parent has a saved destination mapping. Otherwise send without a reply. Linked discussion comments and cross-channel replies are not copied."}[action]
            return await self.show(f"{action.title()}: {'ON' if cfg[action] else 'OFF'}\n{explanation}", [[("Toggle", "toggle:" + action)]])
        if action in {"toggle:formatting", "toggle:media", "toggle:replies"}:
            key = action.split(":")[1]
            await self.set_settings(r.id, **{key: not cfg[key]})
            return await self._callback(key)
        if action == "duration":
            return await self.show("Duration starts on Start, not when selected. Changes apply on the next manual Start. A restart does not extend the saved expiry.", [
                [("10 Minutes", "duration:600"), ("30 Minutes", "duration:1800")],
                [("1 Hour", "duration:3600"), ("6 Hours", "duration:21600")],
                [("24 Hours", "duration:86400"), ("Until I Stop It", "duration:0")],
                [("Custom Duration", "custom_duration")]])
        if action == "custom_duration":
            return await self.prompt("duration", "Enter a duration like 45m, 2h, 3d or 120s (maximum 365 days).", rid=r.id)
        if re.fullmatch(r"duration:(0|600|1800|3600|21600|86400)", action):
            await self.set_settings(r.id, duration=int(action.split(":")[1]))
            return await self.show("Duration saved for the next Start.")
        raise UserFacingError("Unknown or expired button. Use /start.")

    async def message(self, m: Message):
        if m.from_user is None or m.from_user.id != self.owner or m.chat.id != self.owner:
            return
        async with self.admin_lock:
            try:
                if m.text and m.text.split() and m.text.split()[0] in {"/start", "/cancel"}:
                    self.flows = None
                    return await self.home()
                f = self.flows
                if not f:
                    return await self.home()
                value = (m.text or "").strip()
                if not value:
                    raise UserFacingError("Please send text, or Cancel.")
                name = f["name"]
                if name == "dest":
                    kind, ref = parse_reference(value)
                    if kind == "invite":
                        if not self.user:
                            raise UserFacingError("Bots cannot resolve invite links. Add the bot as destination administrator and enter its -100 channel ID, or configure your user connection.")
                        invitation = await self.user(functions.messages.CheckChatInviteRequest(ref))
                        if not isinstance(invitation, types.ChatInviteAlready):
                            raise UserFacingError("For destination invite resolution your account must already be a member. Join in the Telegram app, add the bot as administrator, then retry.")
                        ref = utils.get_peer_id(invitation.chat)
                    chat = await self.check_dest(ref)
                    current = (await self.config()).get("selected", 0)
                    r = await self.route(current)
                    if r and r.dest != chat.id:
                        if r.enabled:
                            raise UserFacingError("Stop the selected mapping before changing its destination.")
                        # Destination changes use a fresh route: old history remains immutable.
                        await self.add_retarget(r, chat)
                    await self.config(destination=dict(id=chat.id, name=chat.title))
                    self.flows = None
                    await self.show(f"Destination Channel: {chat.title}\n{('@'+chat.username) if chat.username else chat.id}\nStatus: ✅ Ready to receive posts", [[("🔄 Change Channel", "dest"), ("🗑 Remove Channel", "remove")]])
                elif name == "source":
                    await self.add_source(value, f["mode"])
                elif name == "change_source":
                    r = await self.route(f["rid"])
                    if not r or r.enabled:
                        raise UserFacingError("Stop the mapping first.")
                    # Replace via the same validated add flow, keeping old history separately.
                    await self.config(destination=dict(id=r.dest, name=r.dest_name))
                    await self.add_source(value, r.mode, replacing=r.id)
                elif name == "rule_old":
                    if not re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,31}", value):
                        raise UserFacingError("Enter a valid @username.")
                    await self.prompt("rule_new", f"Original Username: {value}\nReplace With: enter your @username", rid=f["rid"], old=value)
                elif name == "rule_new":
                    if not re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,31}", value):
                        raise UserFacingError("Enter a valid replacement @username.")
                    r = await self.route(f["rid"])
                    cfg = json.loads(r.settings)
                    rules = [p for p in cfg["rules"] if p[0].lower() != f["old"].lower()]
                    if len(rules) >= 50:
                        raise UserFacingError("Maximum 50 rules per mapping.")
                    rules.append([f["old"], value])
                    await self.set_settings(r.id, rules=rules)
                    self.flows = None
                    await self.replacement_menu()
                elif name == "test":
                    r = await self.route(f["rid"])
                    output, es = transform(m.text, [e.model_dump(exclude_none=True) for e in (m.entities or [])], json.loads(r.settings))
                    await self.bot.send_message(self.owner, output or "[Empty after filtering]", entities=[MessageEntity(**e) for e in es] if output else [])
                    self.flows = None
                elif name == "domains":
                    ds = [d.strip().lower().rstrip(".") for d in value.split(",")]
                    if len(ds) > 50 or not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\.[a-z]{2,63}", d) for d in ds):
                        raise UserFacingError("Enter up to 50 plain domain names, without scheme or paths.")
                    await self.set_settings(f["rid"], domains=sorted(set(ds)), links="block")
                    self.flows = None
                    await self._callback("links")
                elif name == "duration":
                    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h|d)", value.lower())
                    if not match:
                        raise UserFacingError("Use 45m, 2h, 3d or 120s.")
                    seconds = int(match[1]) * dict(s=1,m=60,h=3600,d=86400)[match[2]]
                    if seconds > 365*86400:
                        raise UserFacingError("Maximum duration is 365 days.")
                    await self.set_settings(f["rid"], duration=seconds)
                    self.flows = None
                    await self.show("Duration saved for the next Start.")
                else:
                    raise UserFacingError("Use the confirmation button or Cancel.")
            except Exception as exc:
                await self.show(describe_error(exc))

    async def add_retarget(self, r, chat):
        if chat.id == r.source:
            raise UserFacingError("Source and destination cannot be the same.")
        # Retargeting is not an implicit join: require existing verified access.
        await self.check_source(r)
        previous_cfg = await self.config()
        await self.config(destination=dict(id=chat.id, name=chat.title))
        try:
            await self.add_source(str(r.source), r.mode, replacing=r.id)
        except Exception:
            await self.config(destination=previous_cfg.get("destination"))
            raise

    async def channel_post(self, m: Message):
        await self.ingest(m.chat.id, m.message_id, str(m.media_group_id or ""),
                          m.date.timestamp(), "bot", m.model_dump_json(exclude_none=True))

    async def ingest(self, sid, mid, group, date, mode, payload):
        # Do not backfill downtime or pre-start history. Completed dedupe keys persist.
        for r in await self.routes():
            if r.source != sid or r.mode != mode or not r.enabled:
                continue
            if date < max(r.started, self.ready_at) or (r.expires and stamp() >= r.expires):
                continue
            try:
                async with self.db() as s:
                    prior = await s.scalar(select(Job).where(Job.route == r.id, Job.group_key == group, Job.state.in_(["sending", "sent", "uncertain", "failed"])).limit(1)) if group else None
                    late = bool(prior)
                    s.add(Job(route=r.id, source_message=mid, group_key=group,
                              payload=payload, state="failed" if late else "pending", created=stamp(),
                              error="Late album fragment; not copied" if late else ""))
                    await s.commit()
            except IntegrityError:
                continue
            if late:
                async with self.db() as s:
                    await s.execute(update(Route).where(Route.id == r.id).values(last_error="Late album fragment; not copied"))
                    await s.commit()
                await self.notify("Late album fragment was not copied: " + r.source_name, critical=True)
            self.ensure_worker(r.id)
            self.wakes[r.id].set()

    def ensure_worker(self, rid):
        self.wakes.setdefault(rid, asyncio.Event())
        self.stop_events.setdefault(rid, asyncio.Event())
        if rid not in self.workers or self.workers[rid].done():
            self.workers[rid] = asyncio.create_task(self.worker(rid))

    async def live(self, rid):
        r = await self.route(rid)
        return r and r.enabled and not self.stop_events.setdefault(rid, asyncio.Event()).is_set() and (not r.expires or stamp() < r.expires)

    async def wait_or_stop(self, rid, seconds):
        try:
            await asyncio.wait_for(self.stop_events[rid].wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def worker(self, rid):
        wake = self.wakes[rid]
        try:
            while await self.live(rid):
                wake.clear()
                async with self.db() as s:
                    first = await s.scalar(select(Job).where(Job.route == rid, Job.state == "pending").order_by(Job.source_message).limit(1))
                if not first:
                    await wake.wait()
                    continue
                # Album-only quiet window; no artificial delay for standalone posts.
                if first.group_key:
                    await self.wait_or_stop(rid, 1.0)
                async with self.db() as s:
                    jobs = list((await s.scalars(select(Job).where(Job.route == rid, Job.state == "pending", Job.group_key == first.group_key).order_by(Job.source_message))).all()) if first.group_key else [first]
                if not await self.live(rid):
                    break
                r = await self.route(rid)
                settings = json.loads(r.settings)
                ids = [j.id for j in jobs]
                try:
                    if first.group_key and len(jobs) < 2:
                        raise UserFacingError("Incomplete album received. Post not copied; inspect source and delivery logs.")
                    messages = None
                    for preparation_attempt in range(4):
                        if not await self.live(rid):
                            break
                        try:
                            messages = await self.prepare(r, jobs, settings)
                            break
                        except (TelegramNetworkError, TelegramServerError, OSError, asyncio.TimeoutError, errors.FloodWaitError) as prep_error:
                            if preparation_attempt == 3:
                                raise
                            delay = prep_error.seconds if isinstance(prep_error, errors.FloodWaitError) else 2 ** preparation_attempt
                            if isinstance(prep_error, errors.FloodWaitError):
                                await self.notify("Source account rate limited; waiting before preparing the post.")
                            await self.wait_or_stop(rid, delay)
                    if messages is not None and len(messages) > 1:
                        kinds = {v["kind"] for v in messages}
                        if not (2 <= len(messages) <= 10 and (kinds <= {"photo", "video"} or kinds == {"audio"} or kinds == {"document"})):
                            raise UserFacingError("Unsupported album size or media combination; post not copied.")
                    if messages is None:
                        await self.finish(ids, "skipped", "Media disabled or empty after filtering")
                        continue
                    reply = None
                    parent = messages[0].get("reply")
                    if settings["replies"] and parent:
                        async with self.db() as s:
                            mapped = await s.scalar(select(MessageMap).where(MessageMap.route == rid, MessageMap.source_message == parent))
                        if mapped:
                            reply = ReplyParameters(message_id=mapped.destination_message, allow_sending_without_reply=True)
                    result = None
                    for attempt in range(4):
                        if not await self.live(rid):
                            break
                        await self.finish(ids, "sending", "")
                        try:
                            result = await self.publish(r, messages, reply)
                            break
                        except TelegramRetryAfter as exc:
                            await self.finish(ids, "pending", "Rate limited")
                            await self.notify(f"Telegram rate limit on {r.source_name}; retry after {exc.retry_after} seconds.")
                            if attempt == 3:
                                raise UserFacingError("Repeated Telegram rate limits. Post failed; no automatic replay.")
                            await self.wait_or_stop(rid, exc.retry_after + 0.25)
                    if result is None:
                        await self.finish(ids, "skipped", "Stopped before dispatch")
                        continue
                    if len(result) != len(jobs):
                        raise UserFacingError("Unexpected number of published messages. Manual review required.")
                    async with self.db() as s:
                        for j, out in zip(jobs, result):
                            s.add(MessageMap(route=rid, source_message=j.source_message, destination_message=out.message_id))
                        await s.execute(update(Job).where(Job.id.in_(ids)).values(state="sent", error=""))
                        row = await s.get(Route, rid)
                        if row:
                            row.last_success = stamp()
                            row.last_error = ""
                        await s.commit()
                    await self.notify(f"Published {len(jobs)} post(s): {r.source_name} -> {r.dest_name}", success=True)
                except Exception as exc:
                    # A transport/server failure after dispatch cannot be retried safely.
                    async with self.db() as s:
                        current = list((await s.scalars(select(Job).where(Job.id.in_(ids)))).all())
                    dispatched = any(j.state == "sending" for j in current)
                    uncertain = dispatched and not isinstance(exc, (TelegramBadRequest, TelegramForbiddenError))
                    msg = describe_error(exc)
                    await self.finish(ids, "uncertain" if uncertain else "failed", msg)
                    async with self.db() as s:
                        row = await s.get(Route, rid)
                        if row:
                            row.last_error = msg
                        await s.commit()
                    await self.notify(f"{'Uncertain delivery' if uncertain else 'Post failed'}: {r.source_name}\n{msg}", critical=True)
                    if uncertain or isinstance(exc, TelegramForbiddenError):
                        await self.stop(rid, "Monitoring stopped for manual review.")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.notify("Mapping worker stopped: " + describe_error(exc), critical=True)
            await self.stop(rid, "Worker failure; restart manually after checking configuration.")

    async def finish(self, ids, state, error):
        async with self.db() as s:
            await s.execute(update(Job).where(Job.id.in_(ids)).values(state=state, error=error))
            await s.commit()

    async def prepare(self, r, jobs, settings):
        records = []
        if r.mode == "bot":
            for j in jobs:
                m = Message.model_validate_json(j.payload)
                if m.has_protected_content or getattr(m.chat, "has_protected_content", False):
                    raise UserFacingError("Protected post refused.")
                rec = dict(text=m.text or m.caption or "", entities=[e.model_dump(exclude_none=True) for e in (m.entities or m.caption_entities or [])], reply=m.reply_to_message.message_id if m.reply_to_message and m.reply_to_message.chat.id == m.chat.id else None)
                if m.text is not None:
                    rec["kind"] = "text"
                else:
                    kind = next((k for k in ("photo", "video", "document", "audio") if getattr(m,k)), None)
                    if not kind:
                        raise UserFacingError("Unsupported media/service post. Supported: text, photo, video, document, audio and their supported albums.")
                    media = getattr(m,kind)
                    media = media[-1] if kind == "photo" else media
                    rec.update(kind=kind, file=media.file_id, spoiler=bool(m.has_media_spoiler))
                    if kind == "video":
                        rec.update(width=media.width,height=media.height,duration=media.duration)
                    if kind == "audio":
                        rec.update(duration=media.duration,performer=media.performer,title=media.title)
                records.append(rec)
        else:
            entity = await self.user.get_entity(r.source)
            if getattr(entity, "noforwards", False):
                raise UserFacingError("Protected channel refused.")
            ms = await self.user.get_messages(entity, ids=[j.source_message for j in jobs])
            for m in ms:
                if not m or isinstance(m, types.MessageService):
                    raise UserFacingError("Source post unavailable or unsupported service message.")
                if getattr(m, "noforwards", False):
                    raise UserFacingError("Protected post refused.")
                es = []
                mapping = {"MessageEntityMention":"mention", "MessageEntityHashtag":"hashtag", "MessageEntityCashtag":"cashtag", "MessageEntityBotCommand":"bot_command", "MessageEntityUrl":"url", "MessageEntityEmail":"email", "MessageEntityBold":"bold", "MessageEntityItalic":"italic", "MessageEntityUnderline":"underline", "MessageEntityStrike":"strikethrough", "MessageEntitySpoiler":"spoiler", "MessageEntityCode":"code", "MessageEntityPre":"pre", "MessageEntityTextUrl":"text_link", "MessageEntityCustomEmoji":"custom_emoji", "MessageEntityBlockquote":"blockquote", "MessageEntityPhone":"phone_number"}
                for e in m.entities or []:
                    name = type(e).__name__
                    if name not in mapping:
                        raise UserFacingError("Unsupported user-account entity: " + name)
                    typ = mapping[name]
                    if typ == "blockquote" and getattr(e,"collapsed",False):
                        typ = "expandable_blockquote"
                    d = dict(type=typ,offset=e.offset,length=e.length)
                    if hasattr(e,"url"):
                        d["url"] = e.url
                    if hasattr(e,"language"):
                        d["language"] = e.language
                    if typ == "custom_emoji":
                        d["custom_emoji_id"] = str(e.document_id)
                    es.append(d)
                parent = m.reply_to.reply_to_msg_id if m.reply_to and not m.reply_to.reply_to_peer_id else None
                rec = dict(text=m.message or "",entities=es,reply=parent,kind="text")
                if m.media and not isinstance(m.media, types.MessageMediaWebPage):
                    if not settings["media"]:
                        return None
                    if m.photo:
                        kind, filename, maximum = "photo", "photo.jpg", 10*1024*1024
                        size = max((getattr(s,"size",0) for s in m.photo.sizes), default=0)
                    elif m.document:
                        if m.sticker or m.voice or m.video_note or m.gif:
                            raise UserFacingError("Sticker, voice, round video and animation conversion is not supported.")
                        kind = "video" if m.video else "audio" if m.audio else "document"
                        filename = m.file.name or ("video.mp4" if kind == "video" else "audio.mp3" if kind == "audio" else "document.bin")
                        maximum, size = 50*1024*1024, m.document.size
                    else:
                        raise UserFacingError("This user-account media type cannot be reproduced.")
                    if size > maximum:
                        raise UserFacingError("Media exceeds this Bot API upload path's size limit. Post not copied.")
                    # Bounded in-memory download, including photos with progressive sizes.
                    data = bytearray()
                    async for chunk in self.user.iter_download(m.media):
                        data.extend(chunk)
                        if len(data) > maximum:
                            raise UserFacingError("Media exceeds upload size limit. Post not copied.")
                        if not await self.live(r.id):
                            raise UserFacingError("Stopped during media preparation.")
                    rec.update(kind=kind,file=BufferedInputFile(bytes(data),filename=filename),spoiler=bool(getattr(m.media,"spoiler",False)))
                records.append(rec)
        if any(v["kind"] != "text" for v in records) and not settings["media"]:
            return None
        for rec in records:
            rec["text"], es = transform(rec["text"], rec["entities"], settings)
            rec["entities"] = [MessageEntity(**e) for e in es]
            if rec["kind"] == "text" and not rec["text"].strip():
                return None
        return records

    async def publish(self, r, records, reply):
        if len(records) > 1:
            if not 2 <= len(records) <= 10:
                raise UserFacingError("Unsupported album size; post not copied.")
            kinds = {v["kind"] for v in records}
            if not (kinds <= {"photo","video"} or kinds == {"audio"} or kinds == {"document"}):
                raise UserFacingError("Unsupported album media combination.")
            constructors = dict(photo=InputMediaPhoto,video=InputMediaVideo,document=InputMediaDocument,audio=InputMediaAudio)
            media = []
            for rec in records:
                kwargs = dict(media=rec["file"],caption=rec["text"] or None,caption_entities=rec["entities"])
                if rec["kind"] in {"photo","video"}:
                    kwargs["has_spoiler"] = rec.get("spoiler",False)
                media.append(constructors[rec["kind"]](**kwargs))
            return await self.bot.send_media_group(r.dest,media=media,reply_parameters=reply)
        rec = records[0]
        common = dict(chat_id=r.dest,reply_parameters=reply)
        if rec["kind"] == "text":
            out = await self.bot.send_message(**common,text=rec["text"],entities=rec["entities"])
        else:
            common.update(caption=rec["text"] or None,caption_entities=rec["entities"])
            if rec["kind"] in {"photo","video"}:
                common["has_spoiler"] = rec.get("spoiler",False)
            method = getattr(self.bot,"send_" + rec["kind"])
            out = await method(**common,**{rec["kind"]:rec["file"]})
        return [out]

    async def watchdog(self):
        while True:
            await asyncio.sleep(2)
            if self.pg:
                try:
                    await self.pg.execute(text("SELECT 1"))
                except Exception:
                    # Lock ownership is no longer trustworthy; terminate, never reconnect silently.
                    os._exit(2)
            for r in await self.routes():
                if r.enabled and r.expires and r.expires <= stamp():
                    await self.stop(r.id,"Configured duration expired.")

    async def access_watchdog(self):
        # Permission health checks, not message polling. Events drive ingestion.
        while True:
            await asyncio.sleep(60)
            for r in await self.routes():
                if not r.enabled:
                    continue
                try:
                    await self.check_source(r)
                except (TelegramNetworkError, TelegramServerError, errors.FloodWaitError):
                    continue
                except Exception as exc:
                    await self.stop(r.id,"Source access lost. " + describe_error(exc))
                    await self.notify("Source access lost; monitoring OFF: " + r.source_name,critical=True)
                    continue
                try:
                    await self.check_dest(r.dest)
                except (TelegramNetworkError, TelegramServerError):
                    continue
                except Exception as exc:
                    await self.stop(r.id,"Destination permission lost. " + describe_error(exc))
                    await self.notify("Destination posting permission lost: " + r.dest_name,critical=True)

    async def acquire_lock(self):
        if self.engine.dialect.name == "postgresql":
            self.pg = await self.engine.connect()
            locked = await self.pg.scalar(text("SELECT pg_try_advisory_lock(518907413)"))
            if not locked:
                raise UserFacingError("Another instance is using this database. Run exactly one worker.")
        else:
            import fcntl
            lockpath = Path(os.getenv("LOCK_FILE", "./data/copier.lock"))
            lockpath.parent.mkdir(parents=True,exist_ok=True)
            self.lockfile = open(lockpath,"a+")
            try:
                fcntl.flock(self.lockfile,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                raise UserFacingError("Another local copier is running. Stop it first.")

    async def setup(self):
        await self.acquire_lock()
        async with self.engine.begin() as connection:
            if self.engine.dialect.name == "sqlite":
                await connection.execute(text("PRAGMA journal_mode=WAL"))
            await connection.run_sync(Base.metadata.create_all)
        self.me = await self.bot.get_me()
        path = Path(os.getenv("SESSION_FILE","./data/user.session.enc"))
        if path.exists():
            key = os.environ["SESSION_ENCRYPTION_KEY"]
            session = Fernet(key.encode()).decrypt(path.read_bytes()).decode()
            self.user = TelegramClient(StringSession(session),int(os.environ["API_ID"]),os.environ["API_HASH"],sequential_updates=True,flood_sleep_threshold=0,request_retries=0)
            await self.user.connect()
            if not await self.user.is_user_authorized():
                raise UserFacingError("User session revoked. Authenticate locally again.")
            if (await self.user.get_me()).id != self.owner:
                raise UserFacingError("User session must belong to OWNER_ID. Authenticate your own account.")
            # Populate private-channel access hashes in this in-memory StringSession.
            async for _ in self.user.iter_dialogs():
                pass
            async def on_user(event):
                await self.ingest(event.chat_id,event.id,str(event.grouped_id or ""),event.date.timestamp(),"user",str(event.id))
            self.user.add_event_handler(on_user,events.NewMessage())
        async with self.db() as s:
            # Never blindly retry sends that might already have reached Telegram.
            await s.execute(update(Job).where(Job.state == "sending").values(state="uncertain",error="Process interrupted during send; inspect destination"))
            # No implicit backfill of queued posts from before a restart.
            await s.execute(update(Job).where(Job.state == "pending").values(state="skipped",error="Restart: new posts only"))
            await s.commit()
        self.ready_at = stamp()
        for r in await self.routes():
            if r.enabled:
                try:
                    await self.start(r.id,resume=True)
                except Exception as exc:
                    await self.stop(r.id,"Resume validation failed. " + describe_error(exc))
        self.tasks = [asyncio.create_task(self.watchdog()),asyncio.create_task(self.access_watchdog())]
        # Explicitly discard downtime updates: no automatic old-history copying.
        await self.bot.delete_webhook(drop_pending_updates=True)

    async def run(self):
        try:
            await self.setup()
            await self.dp.start_polling(self.bot,allowed_updates=["message","callback_query","channel_post"],handle_as_tasks=False,close_bot_session=False)
        finally:
            for task in [*self.tasks,*self.workers.values()]:
                task.cancel()
            await asyncio.gather(*self.tasks,*self.workers.values(),return_exceptions=True)
            if self.user:
                await self.user.disconnect()
            if self.pg:
                await self.pg.close()
            await self.engine.dispose()
            await self.bot.session.close()
            if self.lockfile:
                self.lockfile.close()


async def authenticate():
    """Run only in a trusted interactive terminal, never inside Telegram chat."""
    path = Path(os.getenv("SESSION_FILE","./data/user.session.enc"))
    key = os.environ.get("SESSION_ENCRYPTION_KEY")
    if not key:
        raise UserFacingError("Set SESSION_ENCRYPTION_KEY to a Fernet key before authentication. See README.")
    cipher = Fernet(key.encode())
    c = TelegramClient(StringSession(),int(os.environ["API_ID"]),os.environ["API_HASH"],flood_sleep_threshold=0)
    phone = getpass.getpass("Your own Telegram phone number (hidden): ")
    try:
        await c.connect()
        sent = await c.send_code_request(phone)
        code = getpass.getpass("Telegram login code (hidden): ")
        try:
            await c.sign_in(phone,code,phone_code_hash=sent.phone_code_hash)
        except errors.SessionPasswordNeededError:
            await c.sign_in(password=getpass.getpass("Your Telegram two-step password (hidden): "))
        if not await c.is_user_authorized():
            raise UserFacingError("Authorization did not succeed.")
        if (await c.get_me()).id != int(os.environ["OWNER_ID"]):
            raise UserFacingError("This account is not OWNER_ID. Use your own configured account.")
        path.parent.mkdir(parents=True,exist_ok=True)
        temp = path.with_suffix(".tmp")
        fd = os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        with os.fdopen(fd,"wb") as output:
            output.write(cipher.encrypt(c.session.save().encode()))
        os.replace(temp,path)
        os.chmod(path,0o600)
        print("Encrypted session saved. No credentials were printed. Protect the file and key separately.")
    finally:
        await c.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(authenticate() if len(sys.argv)>1 and sys.argv[1]=="auth" else Copier().run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print("Startup stopped: " + describe_error(exc),file=sys.stderr)
        sys.exit(1)
