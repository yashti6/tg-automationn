from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import asyncio
import json
import os
import re
import logging
import threading
import time
import uuid
import secrets
import string

try:
    import socks
    SOCKS_AVAILABLE = True
except ImportError:
    SOCKS_AVAILABLE = False

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, PhoneCodeInvalidError, SessionPasswordNeededError,
    PhoneCodeExpiredError, UserAlreadyParticipantError, ChatAdminRequiredError,
    ChannelPrivateError, UsernameInvalidError
)
from telethon.tl import functions, types
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import ReportRequest as MsgReportRequest, SendReactionRequest
from telethon.tl.functions.account import ReportPeerRequest
from telethon.tl.types import ReportResultChooseOption, ReportResultReported, ReportResultAddComment

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'tg-auto-secret-2024-change-me')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///telegram_automation.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=365)

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pending_logins = {}
task_progress = {}


# ─── MODELS ──────────────────────────────────────────────────────────────────

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_banned = db.Column(db.Boolean, default=False)
    ban_reason = db.Column(db.String(300), nullable=True)
    last_login_at = db.Column(db.DateTime, nullable=True)
    last_login_ip = db.Column(db.String(60), nullable=True)
    accounts = db.relationship('TelegramAccount', backref='user', lazy=True, cascade='all, delete-orphan')
    logs = db.relationship('ActivityLog', backref='user', lazy=True, cascade='all, delete-orphan')
    tasks = db.relationship('BroadcastTask', backref='user', lazy=True, cascade='all, delete-orphan')


class TelegramAccount(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    session_string = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_used = db.Column(db.DateTime, default=datetime.utcnow)
    messages_sent_today = db.Column(db.Integer, default=0)
    reports_sent_today = db.Column(db.Integer, default=0)
    members_scraped = db.Column(db.Integer, default=0)
    members_added = db.Column(db.Integer, default=0)
    total_messages = db.Column(db.Integer, default=0)
    total_reports = db.Column(db.Integer, default=0)


class ActivityLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    action = db.Column(db.String(500), nullable=False)
    status = db.Column(db.String(50), default='success')
    details = db.Column(db.Text)
    account_id = db.Column(db.Integer, nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)


class BroadcastTask(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    message_type = db.Column(db.String(20))
    content = db.Column(db.Text)
    groups = db.Column(db.Text)
    delay = db.Column(db.Integer, default=2)
    status = db.Column(db.String(50), default='pending')
    sent_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)


class ScheduledBroadcast(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    message_type = db.Column(db.String(20), default='text')
    content = db.Column(db.Text, nullable=False)
    caption = db.Column(db.Text, nullable=True)
    groups = db.Column(db.Text, nullable=False)
    delay = db.Column(db.Integer, default=3)
    scheduled_at = db.Column(db.DateTime, nullable=False)
    status = db.Column(db.String(50), default='pending')
    sent_count = db.Column(db.Integer, default=0)
    repeat_interval_minutes = db.Column(db.Integer, nullable=True)  # None = one-shot, else repeat
    next_run_at = db.Column(db.DateTime, nullable=True)
    run_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)


class ProxyConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, nullable=True)
    proxy_type = db.Column(db.String(10), default='socks5')
    host = db.Column(db.String(200), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    username = db.Column(db.String(100), nullable=True)
    password = db.Column(db.String(100), nullable=True)
    is_active = db.Column(db.Boolean, default=True)


class Settings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    api_id = db.Column(db.String(30), nullable=True)
    api_hash = db.Column(db.String(100), nullable=True)


class Subscription(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    plan = db.Column(db.String(20), default='free')  # free, basic, pro, unlimited
    expires_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)


class LicenseKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    plan = db.Column(db.String(20), default='basic')
    duration_days = db.Column(db.Integer, default=30)
    max_uses = db.Column(db.Integer, default=1)
    use_count = db.Column(db.Integer, default=0)
    used_by_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    used_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    notes = db.Column(db.String(300), nullable=True)


class PaymentMethod(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    currency = db.Column(db.String(30), nullable=False)
    network = db.Column(db.String(30), nullable=True)
    address = db.Column(db.String(200), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    display_order = db.Column(db.Integer, default=0)


class PaymentRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    plan = db.Column(db.String(20), nullable=False)
    duration_days = db.Column(db.Integer, default=30)
    amount_usd = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(30), nullable=False)
    txid = db.Column(db.String(200), nullable=False)
    status = db.Column(db.String(20), default='pending')  # pending, approved, rejected
    notes = db.Column(db.String(500), nullable=True)
    submitted_at = db.Column(db.DateTime, default=datetime.utcnow)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    reviewed_by = db.Column(db.String(80), nullable=True)


class LoginHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    ip_address = db.Column(db.String(60), nullable=True)
    user_agent = db.Column(db.String(300), nullable=True)
    status = db.Column(db.String(20), default='success')  # success, failed, banned
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)


class AutoReplyRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    keyword = db.Column(db.String(200), nullable=False)
    reply_text = db.Column(db.Text, nullable=False)
    match_type = db.Column(db.String(20), default='contains')  # contains, exact, starts_with
    is_active = db.Column(db.Boolean, default=True)
    trigger_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class MessageBlacklist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    identifier = db.Column(db.String(200), nullable=False)  # username or user_id
    reason = db.Column(db.String(300), nullable=True)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)


# ─── PLAN LIMITS ──────────────────────────────────────────────────────────────

PLAN_LIMITS = {
    'free':      {'accounts': 6,    'msgs_per_day': 100,   'label': 'Free',      'color': '#8b949e', 'price': '$0/mo',      'price_usdt': 0},
    'basic':     {'accounts': 15,   'msgs_per_day': 2000,  'label': 'Basic',     'color': '#3fb950', 'price': '$29.99/mo',  'price_usdt': 29.99},
    'pro':       {'accounts': 35,   'msgs_per_day': 8000,  'label': 'Pro',       'color': '#00d4ff', 'price': '$59.99/mo',  'price_usdt': 59.99},
    'unlimited': {'accounts': 9999, 'msgs_per_day': 999999,'label': 'Unlimited', 'color': '#f0c040', 'price': '$99.99/mo',  'price_usdt': 99.99},
}

PLAN_FEATURES = {
    'free':      ['broadcast', 'personal'],
    'basic':     ['broadcast', 'personal', 'dms', 'schedule', 'autojoin', 'scraper', 'actools'],
    'pro':       ['all'],
    'unlimited': ['all'],
}

ADMIN_USERS = {'admin'}


def is_admin(user=None):
    u = user or current_user
    return u.is_authenticated and u.username in ADMIN_USERS


def get_user_subscription(user_id):
    user = User.query.get(user_id)
    if user and user.username in ADMIN_USERS:
        return {'plan': 'unlimited', 'active': True, 'expires_at': None, 'expired': False}
    sub = Subscription.query.filter_by(user_id=user_id).first()
    if not sub:
        return {'plan': 'free', 'active': True, 'expires_at': None, 'expired': False}
    if sub.plan != 'unlimited' and sub.expires_at and sub.expires_at < datetime.utcnow():
        return {'plan': 'free', 'active': False, 'expires_at': sub.expires_at, 'expired': True,
                'original_plan': sub.plan}
    return {'plan': sub.plan, 'active': True, 'expires_at': sub.expires_at, 'expired': False}


def require_plan(min_plan):
    """Return error dict if current user doesn't have required plan, else None."""
    if is_admin():
        return None
    order = ['free', 'basic', 'pro', 'unlimited']
    sub = get_user_subscription(current_user.id)
    plan = sub['plan']
    if order.index(plan) < order.index(min_plan):
        return {'success': False, 'error': f'This feature requires {min_plan.capitalize()} plan or higher. Your plan: {plan.capitalize()}', 'upgrade_required': True}
    return None


def check_account_limit():
    """Return error dict if user exceeded their account limit. Admin always passes."""
    if is_admin():
        return None
    sub = get_user_subscription(current_user.id)
    limit = PLAN_LIMITS[sub['plan']]['accounts']
    count = TelegramAccount.query.filter_by(user_id=current_user.id).count()
    if count >= limit:
        return {'success': False, 'error': f'Account limit reached ({count}/{limit}). Upgrade your plan to add more accounts.', 'upgrade_required': True}
    return None


def generate_license_key():
    chars = string.ascii_uppercase + string.digits
    parts = [''.join(secrets.choice(chars) for _ in range(5)) for _ in range(4)]
    return '-'.join(parts)


# ─── HELPERS ─────────────────────────────────────────────────────────────────

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def get_api_credentials(user_id=None):
    if user_id:
        try:
            settings = Settings.query.filter_by(user_id=user_id).first()
            if settings and settings.api_id and settings.api_hash:
                return int(settings.api_id), settings.api_hash
        except Exception:
            pass
    api_id = os.environ.get('TELEGRAM_API_ID', '')
    api_hash = os.environ.get('TELEGRAM_API_HASH', '')
    if not api_id or not api_hash or api_id in ('your-api-id-from-my.telegram.org', 'your-api-id'):
        return None, None
    try:
        return int(api_id), api_hash
    except ValueError:
        return None, None


def get_proxy_for_account(user_id, account_id=None):
    """Return (proxy_tuple, rdns) or None."""
    if not SOCKS_AVAILABLE:
        return None
    try:
        cfg = None
        if account_id:
            cfg = ProxyConfig.query.filter_by(user_id=user_id, account_id=account_id, is_active=True).first()
        if not cfg:
            cfg = ProxyConfig.query.filter_by(user_id=user_id, account_id=None, is_active=True).first()
        if not cfg:
            return None
        proxy_type_map = {
            'socks5': socks.SOCKS5,
            'socks4': socks.SOCKS4,
            'http': socks.HTTP,
        }
        ptype = proxy_type_map.get(cfg.proxy_type, socks.SOCKS5)
        if cfg.username and cfg.password:
            return (ptype, cfg.host, cfg.port, True, cfg.username, cfg.password)
        return (ptype, cfg.host, cfg.port)
    except Exception:
        return None


def make_client(session_str, api_id, api_hash, proxy=None):
    return TelegramClient(StringSession(session_str), api_id, api_hash, proxy=proxy)


def can_access_account(account):
    """Admin can access any account; regular users can only access their own."""
    if is_admin():
        return True
    return account.user_id == current_user.id


def get_account_api_credentials(account):
    """Get API credentials for the account's owner (admin may access any account)."""
    api_id, api_hash = get_api_credentials(account.user_id)
    if not api_id and is_admin():
        api_id, api_hash = get_api_credentials(current_user.id)
    return api_id, api_hash


def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def log_activity(user_id, action, status='success', account_id=None, details=None):
    try:
        with app.app_context():
            log = ActivityLog(
                user_id=user_id,
                action=action,
                status=status,
                account_id=account_id,
                details=details
            )
            db.session.add(log)
            db.session.commit()
    except Exception as e:
        logger.error(f"Error logging activity: {e}")


def parse_post_url(url):
    url = url.strip()
    m = re.match(r'https?://t\.me/c/(\d+)/(\d+)', url)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.match(r'https?://t\.me/([^/]+)/(\d+)', url)
    if m:
        return m.group(1), int(m.group(2))
    return url, None


# ─── AUTH ROUTES ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr or '').split(',')[0].strip()
        ua = request.headers.get('User-Agent', '')[:300]
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            if getattr(user, 'is_banned', False):
                reason = user.ban_reason or 'No reason given'
                try:
                    lh = LoginHistory(user_id=user.id, ip_address=ip_addr, user_agent=ua, status='banned')
                    db.session.add(lh)
                    db.session.commit()
                except Exception:
                    pass
                return render_template('login.html', error=f'Your account has been banned. Reason: {reason}')
            try:
                user.last_login_at = datetime.utcnow()
                user.last_login_ip = ip_addr
                lh = LoginHistory(user_id=user.id, ip_address=ip_addr, user_agent=ua, status='success')
                db.session.add(lh)
                db.session.commit()
            except Exception:
                pass
            login_user(user, remember=True)
            return redirect(url_for('dashboard'))
        # Failed login
        if user:
            try:
                lh = LoginHistory(user_id=user.id, ip_address=ip_addr, user_agent=ua, status='failed')
                db.session.add(lh)
                db.session.commit()
            except Exception:
                pass
        return render_template('login.html', error='Invalid username or password')
    return render_template('login.html')


@app.route('/register', methods=['POST'])
def register():
    try:
        # Accept both JSON and form-encoded data
        ct = request.content_type or ''
        if 'application/json' in ct:
            data = request.get_json() or {}
            username = str(data.get('username', '')).strip()
            password = str(data.get('password', ''))
        else:
            username = str(request.form.get('username') or '').strip()
            password = str(request.form.get('password') or '')
        if not username or not password:
            return jsonify({'error': 'Username and password required'}), 400
        if len(username) < 3:
            return jsonify({'error': 'Username must be at least 3 characters'}), 400
        if len(password) < 4:
            return jsonify({'error': 'Password must be at least 4 characters'}), 400
        if User.query.filter_by(username=username).first():
            return jsonify({'error': 'Username already exists. Please choose another.'}), 400
        user = User(username=username, password=generate_password_hash(password))
        db.session.add(user)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Account created! You can now sign in.'})
    except Exception as e:
        logger.error(f"Register error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', user=current_user)


# ─── SETTINGS / CREDENTIALS ──────────────────────────────────────────────────

@app.route('/api/check_credentials')
@login_required
def check_credentials():
    api_id, api_hash = get_api_credentials(current_user.id)
    settings = Settings.query.filter_by(user_id=current_user.id).first()
    source = 'database' if (settings and settings.api_id) else ('env' if api_id else 'none')
    return jsonify({
        'configured': api_id is not None,
        'api_id': str(settings.api_id) if settings and settings.api_id else os.environ.get('TELEGRAM_API_ID', ''),
        'source': source
    })


@app.route('/api/save_credentials', methods=['POST'])
@login_required
def save_credentials():
    try:
        data = request.get_json()
        api_id_str = str(data.get('api_id', '')).strip()
        api_hash_str = str(data.get('api_hash', '')).strip()
        if not api_id_str or not api_hash_str:
            return jsonify({'success': False, 'error': 'Both API ID and API Hash are required'})
        try:
            int(api_id_str)
        except ValueError:
            return jsonify({'success': False, 'error': 'API ID must be a number'})
        settings = Settings.query.filter_by(user_id=current_user.id).first()
        if not settings:
            settings = Settings(user_id=current_user.id)
            db.session.add(settings)
        settings.api_id = api_id_str
        settings.api_hash = api_hash_str
        db.session.commit()
        log_activity(current_user.id, "Saved Telegram API credentials", 'success')
        return jsonify({'success': True, 'message': 'API credentials saved successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── PROXY SETTINGS ──────────────────────────────────────────────────────────

@app.route('/api/save_proxy', methods=['POST'])
@login_required
def save_proxy():
    try:
        data = request.get_json()
        proxy_type = data.get('proxy_type', 'socks5')
        host = data.get('host', '').strip()
        port = int(data.get('port', 0))
        username = data.get('username', '').strip() or None
        password = data.get('password', '').strip() or None
        account_id = data.get('account_id') or None
        if account_id:
            account_id = int(account_id)

        if not host or not port:
            return jsonify({'success': False, 'error': 'Host and port required'})

        cfg = ProxyConfig.query.filter_by(user_id=current_user.id, account_id=account_id).first()
        if not cfg:
            cfg = ProxyConfig(user_id=current_user.id, account_id=account_id)
            db.session.add(cfg)
        cfg.proxy_type = proxy_type
        cfg.host = host
        cfg.port = port
        cfg.username = username
        cfg.password = password
        cfg.is_active = True
        db.session.commit()
        return jsonify({'success': True, 'message': 'Proxy saved'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_proxy')
@login_required
def get_proxy():
    try:
        cfgs = ProxyConfig.query.filter_by(user_id=current_user.id).all()
        return jsonify({'success': True, 'proxies': [{
            'id': c.id, 'account_id': c.account_id, 'proxy_type': c.proxy_type,
            'host': c.host, 'port': c.port, 'username': c.username, 'is_active': c.is_active
        } for c in cfgs]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/delete_proxy/<int:proxy_id>', methods=['DELETE'])
@login_required
def delete_proxy(proxy_id):
    try:
        cfg = ProxyConfig.query.get_or_404(proxy_id)
        if cfg.user_id != current_user.id:
            return jsonify({'error': 'Unauthorized'}), 403
        db.session.delete(cfg)
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── ACCOUNT MANAGEMENT ──────────────────────────────────────────────────────

@app.route('/api/send_code', methods=['POST'])
@login_required
def send_code():
    try:
        # Check account limit based on plan
        limit_err = check_account_limit()
        if limit_err:
            return jsonify(limit_err)

        data = request.get_json()
        phone = data.get('phone', '').strip()
        if not phone:
            return jsonify({'success': False, 'error': 'Phone number required'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured. Please enter your API ID and API Hash above.'})

        existing = TelegramAccount.query.filter_by(user_id=current_user.id, phone=phone).first()
        if existing:
            return jsonify({'success': False, 'error': 'This phone number is already added'})

        proxy = get_proxy_for_account(current_user.id)

        async def _send():
            client = TelegramClient(StringSession(), api_id, api_hash, proxy=proxy)
            await client.connect()
            result = await client.send_code_request(phone)
            session_after = client.session.save()
            await client.disconnect()
            return result.phone_code_hash, session_after

        phone_code_hash, session_after = run_async(_send())
        pending_logins[phone] = {
            'phone_code_hash': phone_code_hash,
            'session_after_send': session_after,
            'user_id': current_user.id,
            'api_id': api_id,
            'api_hash': api_hash
        }

        return jsonify({'success': True, 'message': f'Verification code sent to {phone}'})

    except Exception as e:
        logger.error(f"send_code error: {e}")
        err_str = str(e)
        if 'api_id' in err_str.lower() or 'api_hash' in err_str.lower() or 'combination is invalid' in err_str.lower():
            err_str = 'Invalid API ID or API Hash. Please get your credentials from my.telegram.org and re-save them.'
        elif 'flood' in err_str.lower() or 'FloodWait' in err_str:
            err_str = 'Too many attempts. Please wait a few minutes before trying again.'
        elif 'phone number invalid' in err_str.lower() or 'PhoneNumberInvalid' in err_str:
            err_str = 'Invalid phone number. Use international format, e.g. +8801XXXXXXXXX'
        elif 'network' in err_str.lower() or 'connect' in err_str.lower() or 'timeout' in err_str.lower():
            err_str = 'Connection error. Check your proxy settings or try again.'
        return jsonify({'success': False, 'error': err_str})


@app.route('/api/verify_code', methods=['POST'])
@login_required
def verify_code():
    try:
        data = request.get_json()
        phone = data.get('phone', '').strip()
        code = data.get('code', '').strip()

        if not phone or not code:
            return jsonify({'success': False, 'error': 'Phone and code required'})

        if phone not in pending_logins:
            return jsonify({'success': False, 'error': 'No pending login for this phone. Send code first.'})

        info = pending_logins[phone]
        api_id = info['api_id']
        api_hash = info['api_hash']
        phone_code_hash = info['phone_code_hash']
        proxy = get_proxy_for_account(current_user.id)

        async def _verify():
            client = TelegramClient(StringSession(info['session_after_send']), api_id, api_hash, proxy=proxy)
            await client.connect()
            try:
                await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
                session_string = client.session.save()
                me = await client.get_me()
                await client.disconnect()
                return {'done': True, 'session': session_string, 'name': f"{me.first_name or ''} {me.last_name or ''}".strip(), 'username': me.username}
            except SessionPasswordNeededError:
                partial = client.session.save()
                await client.disconnect()
                return {'done': False, 'needs_2fa': True, 'partial_session': partial}

        result = run_async(_verify())

        if result.get('needs_2fa'):
            pending_logins[phone]['partial_session'] = result['partial_session']
            return jsonify({'success': True, 'needs_2fa': True, 'message': '2FA password required'})

        account = TelegramAccount(
            user_id=current_user.id,
            phone=phone,
            session_string=result['session'],
            is_active=True
        )
        db.session.add(account)
        db.session.commit()
        del pending_logins[phone]

        log_activity(current_user.id, f"Connected Telegram account {phone} ({result.get('name', '')})", 'success', account.id)
        return jsonify({'success': True, 'message': f"Account {phone} connected successfully!"})

    except PhoneCodeInvalidError:
        return jsonify({'success': False, 'error': 'Invalid verification code. Please try again.'})
    except PhoneCodeExpiredError:
        if phone in pending_logins:
            del pending_logins[phone]
        return jsonify({'success': False, 'error': 'Code expired. Please click "Send Code" again.'})
    except Exception as e:
        logger.error(f"verify_code error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/verify_2fa', methods=['POST'])
@login_required
def verify_2fa():
    try:
        data = request.get_json()
        phone = data.get('phone', '').strip()
        password = data.get('password', '').strip()

        if phone not in pending_logins or 'partial_session' not in pending_logins[phone]:
            return jsonify({'success': False, 'error': 'Session expired. Please start over.'})

        info = pending_logins[phone]
        api_id = info['api_id']
        api_hash = info['api_hash']
        partial_session = info['partial_session']
        proxy = get_proxy_for_account(current_user.id)

        async def _2fa():
            client = TelegramClient(StringSession(partial_session), api_id, api_hash, proxy=proxy)
            await client.connect()
            await client.sign_in(password=password)
            session_string = client.session.save()
            me = await client.get_me()
            await client.disconnect()
            return session_string, f"{me.first_name or ''} {me.last_name or ''}".strip()

        session_string, name = run_async(_2fa())

        account = TelegramAccount(
            user_id=current_user.id,
            phone=phone,
            session_string=session_string,
            is_active=True
        )
        db.session.add(account)
        db.session.commit()
        del pending_logins[phone]

        log_activity(current_user.id, f"Connected Telegram account {phone} ({name}) with 2FA", 'success', account.id)
        return jsonify({'success': True, 'message': f"Account {phone} connected successfully!"})

    except Exception as e:
        logger.error(f"verify_2fa error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/remove_account/<int:account_id>', methods=['DELETE'])
@login_required
def remove_account(account_id):
    try:
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        phone = account.phone
        db.session.delete(account)
        db.session.commit()
        log_activity(current_user.id, f"Removed account {phone}", 'success')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── GROUPS ──────────────────────────────────────────────────────────────────

@app.route('/api/get_groups/<int:account_id>')
@login_required
def get_groups(account_id):
    try:
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'error': 'Account not properly connected. Please re-add this account.'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _get_groups():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            me = await client.get_me()
            groups = []
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    ent = dialog.entity
                    is_broadcast = getattr(ent, 'broadcast', False) and not getattr(ent, 'megagroup', False)
                    default_banned = getattr(ent, 'default_banned_rights', None)
                    msgs_banned = getattr(default_banned, 'send_messages', False) if default_banned else False
                    can_send = not is_broadcast and not msgs_banned
                    groups.append({
                        'id': dialog.id,
                        'title': dialog.title,
                        'username': getattr(ent, 'username', None),
                        'participants_count': getattr(ent, 'participants_count', 0) or 0,
                        'is_channel': dialog.is_channel,
                        'is_broadcast': is_broadcast,
                        'can_send': can_send
                    })
            await client.disconnect()
            return groups

        groups = run_async(_get_groups())
        account.last_used = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'groups': groups, 'count': len(groups)})

    except Exception as e:
        logger.error(f"get_groups error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scrape_group_links/<int:account_id>')
@login_required
def scrape_group_links(account_id):
    try:
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'error': 'Account not properly connected'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _scrape_links():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            links = []
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    ent = dialog.entity
                    username = getattr(ent, 'username', None)
                    is_broadcast = getattr(ent, 'broadcast', False) and not getattr(ent, 'megagroup', False)
                    link = f'https://t.me/{username}' if username else None
                    if not link:
                        try:
                            inv = await client(functions.messages.ExportChatInviteRequest(peer=ent))
                            link = inv.link
                        except Exception:
                            link = f'(private, ID: {dialog.id})'
                    links.append({
                        'id': dialog.id,
                        'title': dialog.title,
                        'link': link,
                        'type': 'Channel' if is_broadcast else ('Supergroup' if getattr(ent, 'megagroup', False) else 'Group'),
                        'members': getattr(ent, 'participants_count', 0) or 0
                    })
            await client.disconnect()
            return links

        links = run_async(_scrape_links())
        account.last_used = datetime.utcnow()
        db.session.commit()
        log_activity(current_user.id, f"Scraped {len(links)} group links", 'success', account_id)
        return jsonify({'success': True, 'links': links, 'count': len(links)})

    except Exception as e:
        logger.error(f"scrape_group_links error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/search_keyword_groups', methods=['POST'])
@login_required
def search_keyword_groups():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        keyword = data.get('keyword', '').strip()
        limit = min(int(data.get('limit', 30)), 50)
        join_ids = data.get('join_ids', [])

        if not keyword:
            return jsonify({'success': False, 'error': 'Keyword required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _search_and_join():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            groups = []
            try:
                result = await client(functions.contacts.SearchRequest(q=keyword, limit=limit))
                for chat in result.chats:
                    username = getattr(chat, 'username', None)
                    is_broadcast = getattr(chat, 'broadcast', False)
                    groups.append({
                        'id': chat.id,
                        'title': getattr(chat, 'title', 'Unknown'),
                        'username': username,
                        'participants_count': getattr(chat, 'participants_count', 0) or 0,
                        'is_channel': is_broadcast,
                        'type': 'Channel' if is_broadcast else 'Group',
                        'link': f'https://t.me/{username}' if username else None
                    })
            except Exception as e:
                logger.error(f"search error: {e}")

            joined = 0
            join_errors = []
            for jid in join_ids:
                try:
                    target_chat = next((g for g in groups if str(g['id']) == str(jid) or g['username'] == jid), None)
                    if target_chat and target_chat.get('username'):
                        ent = await client.get_entity(target_chat['username'])
                        await client(functions.channels.JoinChannelRequest(channel=ent))
                        joined += 1
                        await asyncio.sleep(2)
                except UserAlreadyParticipantError:
                    joined += 1
                except Exception as e:
                    join_errors.append(str(e))

            await client.disconnect()
            return groups, joined, join_errors

        groups, joined, join_errors = run_async(_search_and_join())
        account.last_used = datetime.utcnow()
        db.session.commit()

        msg = f"Found {len(groups)} results for '{keyword}'"
        if join_ids:
            msg += f" | Joined: {joined}, Failed: {len(join_errors)}"
        log_activity(current_user.id, msg, 'success', account_id)
        return jsonify({'success': True, 'results': groups, 'count': len(groups), 'joined': joined, 'join_errors': join_errors})

    except Exception as e:
        logger.error(f"search_keyword_groups error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/export_groups', methods=['POST'])
@login_required
def export_groups():
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        accounts = TelegramAccount.query.filter_by(user_id=current_user.id).all()
        if account_ids:
            accounts = [a for a in accounts if a.id in [int(x) for x in account_ids]]

        all_groups = {}
        for account in accounts:
            if not account.session_string or account.session_string in ('demo', 'active'):
                continue
            try:
                proxy = get_proxy_for_account(current_user.id, account.id)
                async def _get(sess=account.session_string, acc_id=account.id):
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=get_proxy_for_account(current_user.id, acc_id))
                    await client.connect()
                    groups = []
                    async for dialog in client.iter_dialogs():
                        if dialog.is_group or dialog.is_channel:
                            ent = dialog.entity
                            username = getattr(ent, 'username', None)
                            groups.append({
                                'id': dialog.id,
                                'title': dialog.title,
                                'username': username,
                                'link': f'https://t.me/{username}' if username else f'(ID: {dialog.id})',
                                'is_channel': dialog.is_channel,
                                'participants': getattr(ent, 'participants_count', 0) or 0
                            })
                    await client.disconnect()
                    return groups
                groups = run_async(_get())
                for g in groups:
                    key = str(g['id'])
                    if key not in all_groups:
                        all_groups[key] = g
                account.last_used = datetime.utcnow()
            except Exception as e:
                logger.error(f"export_groups account {account.phone} error: {e}")

        db.session.commit()
        result = sorted(all_groups.values(), key=lambda x: x.get('title', ''))
        return jsonify({'success': True, 'groups': result, 'count': len(result)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── PERSONAL CHATS ──────────────────────────────────────────────────────────

@app.route('/api/get_personal_chats/<int:account_id>')
@login_required
def get_personal_chats(account_id):
    try:
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'error': 'Account not properly connected'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _get_chats():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            me = await client.get_me()
            chats = []
            async for dialog in client.iter_dialogs():
                if dialog.is_user and not dialog.entity.bot:
                    user = dialog.entity
                    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or f"User {user.id}"
                    chats.append({
                        'id': user.id,
                        'access_hash': user.access_hash,
                        'name': name,
                        'username': user.username,
                        'phone': user.phone,
                        'unread': dialog.unread_count,
                        'last_message': dialog.message.message[:80] if dialog.message and dialog.message.message else '',
                    })
            await client.disconnect()
            return me.id, chats

        my_id, chats = run_async(_get_chats())
        account.last_used = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'chats': chats, 'count': len(chats), 'my_id': my_id})

    except Exception as e:
        logger.error(f"get_personal_chats error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/send_personal_message', methods=['POST'])
@login_required
def send_personal_message():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        target_id = data.get('target_id')
        access_hash = data.get('access_hash')
        username = data.get('username') or None
        message = data.get('message', '').strip()

        if not target_id or not message:
            return jsonify({'success': False, 'error': 'Target and message required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _send():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                if username:
                    # Username is most reliable — Telegram resolves it directly
                    await client.send_message(username, message)
                elif access_hash:
                    # Construct InputPeerUser so Telethon doesn't need to resolve the entity
                    peer = types.InputPeerUser(
                        user_id=int(target_id),
                        access_hash=int(access_hash)
                    )
                    await client.send_message(peer, message)
                else:
                    # Last resort: iterate dialogs to warm cache, then send
                    await client.get_dialogs(limit=1)
                    await client.send_message(int(target_id), message)
            finally:
                await client.disconnect()

        run_async(_send())
        account.messages_sent_today += 1
        account.total_messages += 1
        account.last_used = datetime.utcnow()
        db.session.commit()
        log_activity(current_user.id, f"Sent personal message to user {target_id}", 'success', account_id)
        return jsonify({'success': True, 'message': 'Message sent'})

    except Exception as e:
        logger.error(f"send_personal_message error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── CHAT HISTORY ────────────────────────────────────────────────────────────

@app.route('/api/get_chat_history', methods=['POST'])
@login_required
def get_chat_history():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        user_id = int(data.get('user_id'))
        access_hash = data.get('access_hash')
        username = data.get('username') or None
        limit = min(int(data.get('limit', 50)), 100)

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _get_history():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            me = await client.get_me()
            try:
                if username:
                    peer = username
                elif access_hash:
                    peer = types.InputPeerUser(user_id=user_id, access_hash=int(access_hash))
                else:
                    peer = user_id
                messages = []
                async for msg in client.iter_messages(peer, limit=limit):
                    if msg.text or msg.message:
                        messages.append({
                            'id': msg.id,
                            'text': msg.text or msg.message or '',
                            'date': msg.date.strftime('%Y-%m-%d %H:%M') if msg.date else '',
                            'time': msg.date.strftime('%H:%M') if msg.date else '',
                            'from_me': msg.out,
                        })
                messages.reverse()
                return messages
            finally:
                await client.disconnect()

        messages = run_async(_get_history())
        return jsonify({'success': True, 'messages': messages, 'count': len(messages)})

    except Exception as e:
        logger.error(f"get_chat_history error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── ACCOUNT TOOLS ────────────────────────────────────────────────────────────

@app.route('/api/update_profile', methods=['POST'])
@login_required
def update_profile():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        first_name = data.get('first_name', '').strip()
        last_name = data.get('last_name', '').strip()
        bio = data.get('bio', '').strip()
        username = data.get('username', '').strip()

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _update():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            results = []
            try:
                if first_name or last_name is not None:
                    await client(functions.account.UpdateProfileRequest(
                        first_name=first_name or '',
                        last_name=last_name or '',
                        about=bio
                    ))
                    results.append('Name/Bio updated')
                if username:
                    await client(functions.account.UpdateUsernameRequest(username=username))
                    results.append(f'Username set to @{username}')
            finally:
                await client.disconnect()
            return results

        results = run_async(_update())
        log_activity(current_user.id, f"Profile updated for account {account_id}", 'success', account_id)
        return jsonify({'success': True, 'message': ', '.join(results) if results else 'Profile updated'})

    except Exception as e:
        logger.error(f"update_profile error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_account_info/<int:account_id>')
@login_required
def get_account_info(account_id):
    try:
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _get_info():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                me = await client.get_me()
                full = await client(functions.users.GetFullUserRequest(id='me'))
                return {
                    'id': me.id,
                    'first_name': me.first_name or '',
                    'last_name': me.last_name or '',
                    'username': me.username or '',
                    'phone': me.phone or '',
                    'bio': full.full_user.about or '',
                    'premium': getattr(me, 'premium', False),
                    'dc_id': me.photo.dc_id if me.photo else 'N/A',
                    'restricted': me.restricted or False,
                }
            finally:
                await client.disconnect()

        info = run_async(_get_info())
        return jsonify({'success': True, 'info': info})

    except Exception as e:
        logger.error(f"get_account_info error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/mark_all_read', methods=['POST'])
@login_required
def mark_all_read():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _mark():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            count = 0
            try:
                async for dialog in client.iter_dialogs():
                    if dialog.unread_count > 0:
                        await client.send_read_acknowledge(dialog.entity)
                        count += 1
            finally:
                await client.disconnect()
            return count

        count = run_async(_mark())
        log_activity(current_user.id, f"Marked {count} chats as read", 'success', account_id)
        return jsonify({'success': True, 'message': f'Marked {count} chats as read'})

    except Exception as e:
        logger.error(f"mark_all_read error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/set_online', methods=['POST'])
@login_required
def set_online():
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        online = bool(data.get('online', True))

        if not account_ids:
            accounts = TelegramAccount.query.filter_by(user_id=current_user.id, status='connected').all()
            account_ids = [a.id for a in accounts]

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        results = []
        for aid in account_ids:
            account = TelegramAccount.query.get(aid)
            if not account or not can_access_account(account):
                continue
            if not account.session_string or account.session_string in ('demo', 'active'):
                continue
            proxy = get_proxy_for_account(current_user.id, aid)

            async def _set_online(sess=account.session_string, prx=proxy):
                client = TelegramClient(StringSession(sess), api_id, api_hash_val, proxy=prx)
                await client.connect()
                try:
                    await client(functions.account.UpdateStatusRequest(offline=not online))
                finally:
                    await client.disconnect()

            try:
                run_async(_set_online())
                results.append({'id': aid, 'phone': account.phone, 'status': 'ok'})
            except Exception as e:
                results.append({'id': aid, 'phone': account.phone, 'status': str(e)})

        status_word = 'Online' if online else 'Offline'
        log_activity(current_user.id, f"Set {len(results)} account(s) {status_word}", 'success')
        return jsonify({'success': True, 'message': f'{len(results)} account(s) set to {status_word}', 'results': results})

    except Exception as e:
        logger.error(f"set_online error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/forward_message', methods=['POST'])
@login_required
def forward_message():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        from_chat = data.get('from_chat', '').strip()
        msg_id = int(data.get('msg_id', 0))
        to_chats = [c.strip() for c in data.get('to_chats', '').split('\n') if c.strip()]

        if not from_chat or not msg_id or not to_chats:
            return jsonify({'success': False, 'error': 'Source chat, message ID, and destinations required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _forward():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            ok, fail = 0, 0
            try:
                src = await client.get_entity(from_chat)
                for dest in to_chats:
                    try:
                        dst = await client.get_entity(dest)
                        await client.forward_messages(dst, msg_id, src)
                        ok += 1
                        await asyncio.sleep(2)
                    except Exception as e:
                        logger.warning(f"Forward to {dest} failed: {e}")
                        fail += 1
            finally:
                await client.disconnect()
            return ok, fail

        ok, fail = run_async(_forward())
        log_activity(current_user.id, f"Forwarded msg {msg_id} to {ok} chats", 'success', account_id)
        return jsonify({'success': True, 'message': f'Forwarded to {ok} chats, {fail} failed'})

    except Exception as e:
        logger.error(f"forward_message error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── BROADCAST ───────────────────────────────────────────────────────────────

@app.route('/api/broadcast', methods=['POST'])
@login_required
def broadcast():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        message_type = data.get('type', 'text')
        content = data.get('content', '').strip()
        caption = data.get('caption', '').strip()
        group_ids = data.get('group_ids', [])
        delay = max(1, int(data.get('delay', 3)))

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not content:
            return jsonify({'success': False, 'error': 'Content required'}), 400
        if not group_ids:
            return jsonify({'success': False, 'error': 'Select at least one group'}), 400
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected. Please re-add it.'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        task = BroadcastTask(
            user_id=current_user.id,
            account_id=account_id,
            message_type=message_type,
            content=content,
            groups=json.dumps(group_ids),
            delay=delay,
            status='running'
        )
        db.session.add(task)
        db.session.commit()
        task_id = task.id
        task_progress[task_id] = {'sent': 0, 'failed': 0, 'total': len(group_ids), 'status': 'running', 'errors': []}
        user_id = current_user.id
        sess = account.session_string

        def run_broadcast():
            with app.app_context():
                proxy = get_proxy_for_account(user_id, account_id)
                async def _broadcast():
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    sent = 0
                    failed = 0
                    for group_id in group_ids:
                        try:
                            entity = await client.get_entity(int(group_id))
                            if message_type == 'text':
                                await client.send_message(entity, content)
                            elif message_type in ('photo', 'video'):
                                await client.send_file(entity, content, caption=caption or content)
                            sent += 1
                            task_progress[task_id]['sent'] = sent
                            await asyncio.sleep(delay)
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            failed += 1
                            task_progress[task_id]['failed'] = failed
                            task_progress[task_id]['errors'].append(str(e))
                    await client.disconnect()
                    return sent, failed

                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    sent, failed = loop.run_until_complete(_broadcast())
                    loop.close()
                    t = BroadcastTask.query.get(task_id)
                    acc = TelegramAccount.query.get(account_id)
                    if t:
                        t.sent_count = sent
                        t.status = 'completed'
                        t.completed_at = datetime.utcnow()
                    if acc:
                        acc.messages_sent_today += sent
                        acc.total_messages += sent
                        acc.last_used = datetime.utcnow()
                    db.session.commit()
                    task_progress[task_id]['status'] = 'completed'
                    log_activity(user_id, f"Broadcast completed: {sent} sent, {failed} failed", 'success', account_id)
                except Exception as e:
                    t = BroadcastTask.query.get(task_id)
                    if t:
                        t.status = 'failed'
                        t.completed_at = datetime.utcnow()
                        db.session.commit()
                    task_progress[task_id]['status'] = 'failed'
                    logger.error(f"Broadcast thread error: {e}")

        threading.Thread(target=run_broadcast, daemon=True).start()
        return jsonify({'success': True, 'message': f'Broadcast started to {len(group_ids)} groups', 'task_id': task_id})

    except Exception as e:
        logger.error(f"broadcast error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/task_status/<int:task_id>')
@login_required
def task_status(task_id):
    progress = task_progress.get(task_id, {})
    task = BroadcastTask.query.get(task_id)
    if task and task.user_id != current_user.id:
        return jsonify({'error': 'Unauthorized'}), 403
    return jsonify({'success': True, 'progress': progress})


# ─── SCHEDULED BROADCAST ─────────────────────────────────────────────────────

@app.route('/api/schedule_broadcast', methods=['POST'])
@login_required
def schedule_broadcast():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        message_type = data.get('type', 'text')
        content = data.get('content', '').strip()
        caption = data.get('caption', '').strip()
        group_ids = data.get('group_ids', [])
        delay = max(1, int(data.get('delay', 3)))
        scheduled_at_str = data.get('scheduled_at', '')

        if not content or not group_ids or not scheduled_at_str:
            return jsonify({'success': False, 'error': 'Content, groups, and schedule time required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403

        try:
            scheduled_at = datetime.fromisoformat(scheduled_at_str)
        except Exception:
            return jsonify({'success': False, 'error': 'Invalid date/time format'})

        repeat_interval_minutes = data.get('repeat_interval_minutes')
        if repeat_interval_minutes:
            repeat_interval_minutes = max(1, int(repeat_interval_minutes))
        else:
            repeat_interval_minutes = None
            if scheduled_at <= datetime.utcnow():
                return jsonify({'success': False, 'error': 'Schedule time must be in the future'})

        sb = ScheduledBroadcast(
            user_id=current_user.id,
            account_id=account_id,
            message_type=message_type,
            content=content,
            caption=caption or None,
            groups=json.dumps(group_ids),
            delay=delay,
            scheduled_at=scheduled_at,
            status='pending',
            repeat_interval_minutes=repeat_interval_minutes,
            next_run_at=scheduled_at if repeat_interval_minutes else None,
        )
        db.session.add(sb)
        db.session.commit()

        if repeat_interval_minutes:
            msg = f'Interval broadcast created: every {repeat_interval_minutes} min'
            log_activity(current_user.id, f"Interval broadcast every {repeat_interval_minutes}min to {len(group_ids)} groups", 'success', account_id)
        else:
            msg = f'Broadcast scheduled for {scheduled_at.strftime("%Y-%m-%d %H:%M UTC")}'
            log_activity(current_user.id, f"Scheduled broadcast for {scheduled_at.strftime('%Y-%m-%d %H:%M')} to {len(group_ids)} groups", 'success', account_id)
        return jsonify({'success': True, 'message': msg, 'id': sb.id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_scheduled_broadcasts')
@login_required
def get_scheduled_broadcasts():
    try:
        items = ScheduledBroadcast.query.filter_by(user_id=current_user.id).order_by(ScheduledBroadcast.scheduled_at).all()
        result = []
        for sb in items:
            acc = TelegramAccount.query.get(sb.account_id)
            result.append({
                'id': sb.id,
                'account': acc.phone if acc else 'Unknown',
                'message_type': sb.message_type,
                'content': sb.content[:80],
                'groups_count': len(json.loads(sb.groups)),
                'delay': sb.delay,
                'scheduled_at': sb.scheduled_at.isoformat(),
                'status': sb.status,
                'sent_count': sb.sent_count
            })
        return jsonify({'success': True, 'scheduled': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/delete_scheduled/<int:sb_id>', methods=['DELETE'])
@login_required
def delete_scheduled(sb_id):
    try:
        sb = ScheduledBroadcast.query.get_or_404(sb_id)
        if sb.user_id != current_user.id:
            return jsonify({'error': 'Unauthorized'}), 403
        db.session.delete(sb)
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── REPORTING ───────────────────────────────────────────────────────────────

def _parse_tg_target(raw):
    """Parse a Telegram URL/username into (entity_str, msg_id_or_None).
    Handles: @username, t.me/user, t.me/user/123, t.me/c/CHANID/MSGID, plain usernames."""
    import re
    raw = (raw or '').strip()
    if not raw:
        return None, None
    # t.me/c/CHANID/MSGID  — private channel post
    m = re.match(r'(?:https?://)?(?:www\.)?t\.me/c/(\d+)/(\d+)', raw)
    if m:
        return f'-100{m.group(1)}', int(m.group(2))
    # t.me/USERNAME/MSGID — public channel post
    m = re.match(r'(?:https?://)?(?:www\.)?t\.me/([A-Za-z][A-Za-z0-9_]{3,})/(\d+)', raw)
    if m:
        return m.group(1), int(m.group(2))
    # t.me/USERNAME or t.me/+INVITE
    m = re.match(r'(?:https?://)?(?:www\.)?t\.me/([A-Za-z0-9_+][A-Za-z0-9_]{3,})/?$', raw)
    if m:
        slug = m.group(1)
        return (f'@{slug}' if not slug.startswith('+') else slug), None
    # Plain @username or username
    if re.match(r'^@?[A-Za-z][A-Za-z0-9_]{3,}$', raw):
        return raw if raw.startswith('@') else f'@{raw}', None
    # Return as-is and let Telethon try
    return raw, None


_stopped_tasks = set()


@app.route('/api/stop_report', methods=['POST'])
@login_required
def stop_report():
    data = request.get_json() or {}
    task_id = data.get('task_id', '')
    if task_id and task_id in task_progress:
        _stopped_tasks.add(task_id)
        task_progress[task_id]['status'] = 'stopped'
        return jsonify({'success': True, 'message': 'Report task stopped.'})
    return jsonify({'success': False, 'error': 'Task not found'})


@app.route('/api/report', methods=['POST'])
@login_required
def report():
    try:
        data = request.get_json() or {}
        target_url = str(data.get('target_url') or data.get('url') or '').strip()
        report_type = str(data.get('report_type') or data.get('type') or 'spam')
        texts = data.get('texts', [])
        custom_text = str(data.get('custom_text') or '').strip()
        account_ids = data.get('account_ids', [])
        reports_per_account = max(1, min(10, int(data.get('reports_per_account', 1))))
        account_delay = max(0, min(60, int(data.get('account_delay', 3))))
        report_delay = max(1, min(30, int(data.get('report_delay', 3))))
        extra_post_urls = [u.strip() for u in data.get('extra_post_urls', []) if str(u).strip()]

        if not texts and custom_text:
            texts = [custom_text]
        if not texts:
            texts = [f"This account/channel is being reported for {report_type}."]

        if not target_url:
            return jsonify({'success': False, 'error': 'Target URL or username is required'})
        if not account_ids:
            return jsonify({'success': False, 'error': 'Please select at least one account to report with'})

        # Parse the primary target
        primary_entity_str, primary_msg_id = _parse_tg_target(target_url)
        if not primary_entity_str:
            return jsonify({'success': False, 'error': 'Cannot parse target URL — check the format'})

        # Parse any extra post URLs
        extra_targets = []  # list of (entity_str, msg_id)
        for eu in extra_post_urls:
            es, eid = _parse_tg_target(eu)
            if es:
                extra_targets.append((es, eid))

        reason_map = {
            'spam':        types.InputReportReasonSpam(),
            'violence':    types.InputReportReasonViolence(),
            'scam':        types.InputReportReasonOther(),
            'other':       types.InputReportReasonOther(),
            'copyright':   types.InputReportReasonCopyright(),
            'child_abuse': types.InputReportReasonChildAbuse(),
            'pornography': types.InputReportReasonPornography(),
            'fake':        types.InputReportReasonFake(),
            'geo':         types.InputReportReasonGeoIrrelevant(),
        }
        reason = reason_map.get(report_type, types.InputReportReasonSpam())

        # Build valid account list with per-account API credentials
        valid_entries = []
        skipped = []
        for aid in account_ids:
            try:
                acc = TelegramAccount.query.get(int(aid))
            except Exception:
                continue
            if not acc:
                skipped.append(f"Account {aid} not found"); continue
            if not can_access_account(acc):
                skipped.append(f"Account {aid} access denied"); continue
            if not acc.session_string or acc.session_string in ('demo', 'active', ''):
                skipped.append(f"{acc.phone}: not connected"); continue
            acc_api_id, acc_api_hash = get_account_api_credentials(acc)
            if not acc_api_id:
                skipped.append(f"{acc.phone}: no API credentials"); continue
            valid_entries.append({'id': acc.id, 'phone': acc.phone,
                                   'session': acc.session_string,
                                   'api_id': acc_api_id, 'api_hash': acc_api_hash})

        if not valid_entries:
            msg = 'No usable accounts found.'
            if skipped:
                msg += ' Issues: ' + '; '.join(skipped[:3])
            return jsonify({'success': False, 'error': msg})

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {
            'status': 'running', 'current_account': '', 'current_step': '',
            'completed': 0, 'total': len(valid_entries),
            'results': [], 'errors': [], 'total_reports_sent': 0
        }

        user_id = current_user.id
        texts_list = list(texts)

        def _categorize_error(err_str):
            e = err_str.lower()
            if 'flood' in e:
                return 'Rate-limited (FloodWait) — waited and retried'
            if 'auth' in e or 'session' in e or 'unauthorized' in e:
                return 'Session expired — please re-add this account'
            if 'peer_id_invalid' in e or 'could not find' in e or 'nobody' in e:
                return 'Target not found — check the link/username'
            if 'api_id' in e or 'combination' in e:
                return 'Invalid API credentials for this account'
            if 'channel_private' in e or 'chat_write_forbidden' in e:
                return 'Channel is private or restricted'
            if 'user_deactivated' in e:
                return 'Account is deactivated'
            if 'too many' in e:
                return 'Too many requests — slow down'
            return err_str[:120]

        def run_report_sequential():
            with app.app_context():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = []
                text_idx = [0]
                total_sent = [0]

                for idx, entry in enumerate(valid_entries):
                    if task_id in _stopped_tasks:
                        break

                    acc = TelegramAccount.query.get(entry['id'])
                    if not acc:
                        continue

                    task_progress[task_id]['current_account'] = acc.phone
                    task_progress[task_id]['current_step'] = 'Connecting…'
                    proxy = get_proxy_for_account(acc.user_id, acc.id)
                    t_idx = text_idx[0]
                    e_api_id = entry['api_id']
                    e_api_hash = entry['api_hash']
                    e_sess = entry['session']

                    async def _report_one(sess=e_sess, aid=e_api_id,
                                          ahash=e_api_hash, tidx=t_idx):
                        client = TelegramClient(StringSession(sess), aid, ahash,
                                                proxy=proxy,
                                                connection_retries=3,
                                                retry_delay=2)
                        reported = 0
                        errors = []

                        async def _do_peer_report(entity, msg_text):
                            await client(ReportPeerRequest(
                                peer=entity, reason=reason, message=msg_text
                            ))

                        async def _do_msg_report(entity, msg_ids, msg_text, rtype_key):
                            keyword_map = {
                                'spam':        ['spam', 'unsolicited', 'advertising'],
                                'violence':    ['violen', 'harm', 'danger', 'threaten'],
                                'scam':        ['scam', 'fraud', 'fake', 'illegal', 'other'],
                                'copyright':   ['copyright', 'intellectual'],
                                'child_abuse': ['child', 'minor', 'csam', 'abuse'],
                                'pornography': ['porn', 'adult', 'sexual', 'explicit', 'nudity'],
                                'fake':        ['fake', 'impersonat'],
                                'geo':         ['geo', 'location', 'irrelevant'],
                            }
                            patterns = keyword_map.get(rtype_key, [rtype_key])
                            option = b''
                            for _step in range(8):
                                res = await client(MsgReportRequest(
                                    peer=entity, id=msg_ids,
                                    option=option, message=msg_text
                                ))
                                if isinstance(res, ReportResultReported):
                                    return True
                                elif isinstance(res, ReportResultAddComment):
                                    opt = getattr(res, 'option', b'')
                                    await client(MsgReportRequest(
                                        peer=entity, id=msg_ids,
                                        option=opt, message=msg_text
                                    ))
                                    return True
                                elif isinstance(res, ReportResultChooseOption):
                                    opts = res.options
                                    chosen = None
                                    for o in opts:
                                        ol = o.text.lower()
                                        if any(p in ol for p in patterns):
                                            chosen = o
                                            break
                                    if not chosen and opts:
                                        chosen = opts[0]
                                    if not chosen:
                                        raise Exception("Telegram returned no report options")
                                    option = chosen.option
                                else:
                                    raise Exception(f"Unexpected result: {type(res).__name__}")
                            raise Exception("Multi-step report did not complete in 8 steps")

                        async def _do_one_target(entity_str, msg_id, rep_idx):
                            nonlocal reported
                            msg_text = texts_list[(tidx + rep_idx) % len(texts_list)]
                            try:
                                entity = await client.get_entity(entity_str)
                                if msg_id:
                                    ok = await _do_msg_report(entity, [msg_id], msg_text, report_type)
                                    if ok:
                                        reported += 1
                                else:
                                    await _do_peer_report(entity, msg_text)
                                    reported += 1
                            except FloodWaitError as fw:
                                wait = min(fw.seconds + 2, 90)
                                errors.append(f'FloodWait {fw.seconds}s — pausing {wait}s')
                                await asyncio.sleep(wait)
                            except Exception as ex:
                                errors.append(_categorize_error(str(ex)))

                        try:
                            await client.connect()
                            if not await client.is_user_authorized():
                                errors.append('Session expired — re-add this account')
                                return reported, errors

                            # Primary target — report N times
                            for i in range(reports_per_account):
                                if task_id in _stopped_tasks:
                                    break
                                task_progress[task_id]['current_step'] = \
                                    f'Report {i+1}/{reports_per_account}'
                                await _do_one_target(primary_entity_str, primary_msg_id, i)
                                task_progress[task_id]['total_reports_sent'] = total_sent[0] + reported
                                if i < reports_per_account - 1:
                                    await asyncio.sleep(report_delay)

                            # Extra post targets — each reported once per account
                            for ei, (es, eid) in enumerate(extra_targets):
                                if task_id in _stopped_tasks:
                                    break
                                task_progress[task_id]['current_step'] = \
                                    f'Extra post {ei+1}/{len(extra_targets)}'
                                await _do_one_target(es, eid, ei)
                                task_progress[task_id]['total_reports_sent'] = total_sent[0] + reported
                                if ei < len(extra_targets) - 1:
                                    await asyncio.sleep(report_delay)

                        except FloodWaitError as fw:
                            errors.append(f'Account flood-waited {fw.seconds}s at connect')
                        except Exception as e:
                            errors.append(_categorize_error(str(e)))
                        finally:
                            try:
                                await client.disconnect()
                            except Exception:
                                pass
                        return reported, errors

                    try:
                        reported, errors = loop.run_until_complete(_report_one())
                        acc.reports_sent_today = (acc.reports_sent_today or 0) + reported
                        acc.total_reports = (acc.total_reports or 0) + reported
                        acc.last_used = datetime.utcnow()
                        db.session.commit()
                        total_sent[0] += reported
                        task_progress[task_id]['total_reports_sent'] = total_sent[0]
                        status_str = 'success' if reported > 0 else ('warning' if errors else 'info')
                        result = {
                            'account': acc.phone, 'success': reported > 0,
                            'reported': reported, 'errors': errors[:6]
                        }
                        log_activity(user_id,
                                     f"Reported {target_url} via {acc.phone}: {reported} sent",
                                     status_str, acc.id)
                    except Exception as e:
                        result = {'account': acc.phone, 'success': False, 'reported': 0,
                                  'errors': [_categorize_error(str(e))]}
                        log_activity(user_id, f"Report failed with {acc.phone}: {e}", 'error', acc.id)

                    text_idx[0] = (text_idx[0] + reports_per_account) % len(texts_list)
                    results.append(result)
                    task_progress[task_id]['completed'] += 1
                    task_progress[task_id]['results'] = results
                    task_progress[task_id]['current_step'] = ''

                    if account_delay > 0 and idx < len(valid_entries) - 1:
                        if task_id not in _stopped_tasks:
                            loop.run_until_complete(asyncio.sleep(account_delay))

                loop.close()
                final_status = 'stopped' if task_id in _stopped_tasks else 'completed'
                task_progress[task_id]['status'] = final_status
                task_progress[task_id]['current_account'] = ''
                task_progress[task_id]['current_step'] = ''
                _stopped_tasks.discard(task_id)

        threading.Thread(target=run_report_sequential, daemon=True).start()
        return jsonify({
            'success': True,
            'message': (f'Reporting started — {len(valid_entries)} account(s), '
                        f'{reports_per_account} report(s)/account, '
                        f'{account_delay}s delay between accounts.'),
            'task_id': task_id,
            'skipped': skipped[:5]
        })

    except Exception as e:
        logger.error(f"Report error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/report_task_status/<task_id>')
@login_required
def report_task_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── REACTIONS ───────────────────────────────────────────────────────────────

@app.route('/api/react', methods=['POST'])
@login_required
def react():
    try:
        data = request.get_json() or {}
        post_url = str(data.get('post_url') or '').strip()
        emoji = str(data.get('emoji') or '👍').strip()
        account_ids = data.get('account_ids', [])
        big = bool(data.get('big', False))
        delay_ms = max(0, int(data.get('delay_ms', 500)))

        if not post_url:
            return jsonify({'success': False, 'error': 'Post URL is required'})
        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'})

        try:
            channel_id, msg_id = parse_post_url(post_url)
            if not msg_id:
                return jsonify({'success': False,
                                'error': 'Invalid post URL — must link to a specific message, e.g. https://t.me/channel/123'})
        except Exception as pe:
            return jsonify({'success': False, 'error': f'Invalid post URL: {pe}'})

        valid_entries = []
        skipped = []
        for aid in account_ids:
            try:
                acc = TelegramAccount.query.get(int(aid))
            except Exception:
                continue
            if not acc or not can_access_account(acc):
                continue
            if not acc.session_string or acc.session_string in ('demo', 'active', ''):
                skipped.append(f"{acc.phone}: not connected")
                continue
            acc_api_id, acc_api_hash = get_account_api_credentials(acc)
            if not acc_api_id:
                skipped.append(f"{acc.phone}: no API credentials")
                continue
            valid_entries.append({'id': acc.id, 'phone': acc.phone,
                                   'session': acc.session_string,
                                   'api_id': acc_api_id, 'api_hash': acc_api_hash})

        if not valid_entries:
            msg = 'No usable accounts.'
            if skipped:
                msg += ' Issues: ' + '; '.join(skipped[:3])
            return jsonify({'success': False, 'error': msg})

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {
            'status': 'running',
            'current_account': '',
            'completed': 0,
            'total': len(valid_entries),
            'results': [],
            'total_reacted': 0
        }

        user_id = current_user.id

        def run_react_sequential():
            with app.app_context():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = []
                total_reacted = [0]

                for entry in valid_entries:
                    acc = TelegramAccount.query.get(entry['id'])
                    if not acc:
                        continue
                    task_progress[task_id]['current_account'] = acc.phone
                    proxy = get_proxy_for_account(acc.user_id, acc.id)

                    async def _react_one(sess=entry['session'], phone=entry['phone'],
                                         aid=entry['api_id'], ahash=entry['api_hash']):
                        client = TelegramClient(StringSession(sess), aid, ahash, proxy=proxy)
                        await client.connect()
                        try:
                            entity = await client.get_entity(channel_id)
                            await client(SendReactionRequest(
                                peer=entity,
                                msg_id=msg_id,
                                big=big,
                                add_to_recent=True,
                                reaction=[types.ReactionEmoji(emoticon=emoji)]
                            ))
                            return True, None
                        except Exception as e:
                            err = str(e)
                            if 'flood' in err.lower():
                                return False, 'Rate limited (FloodWait) — try again later'
                            elif 'reaction' in err.lower() or 'not allowed' in err.lower():
                                return False, 'Reactions not enabled on this post/channel'
                            elif 'auth' in err.lower() or 'unauthorized' in err.lower():
                                return False, 'Session expired — please re-add this account'
                            elif 'could not find' in err.lower() or 'peer' in err.lower():
                                return False, 'Channel/post not found. Check the URL.'
                            elif 'privacy' in err.lower():
                                return False, 'Cannot react due to privacy settings'
                            else:
                                return False, err[:120]
                        finally:
                            try:
                                await client.disconnect()
                            except Exception:
                                pass

                    try:
                        success, error = loop.run_until_complete(_react_one())
                        if success:
                            total_reacted[0] += 1
                            acc.last_used = datetime.utcnow()
                            db.session.commit()
                            result = {'account': acc.phone, 'success': True, 'error': None}
                            log_activity(user_id,
                                         f"Reacted {emoji} on post {msg_id} in {channel_id} with {acc.phone}",
                                         'success', acc.id)
                        else:
                            result = {'account': acc.phone, 'success': False, 'error': error}
                            log_activity(user_id, f"React failed with {acc.phone}: {error}", 'warning', acc.id)
                    except Exception as e:
                        result = {'account': acc.phone, 'success': False, 'error': str(e)[:120]}

                    results.append(result)
                    task_progress[task_id]['completed'] += 1
                    task_progress[task_id]['results'] = results
                    task_progress[task_id]['total_reacted'] = total_reacted[0]
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)

                loop.close()
                task_progress[task_id]['status'] = 'completed'
                task_progress[task_id]['current_account'] = ''

        threading.Thread(target=run_react_sequential, daemon=True).start()
        return jsonify({
            'success': True,
            'message': f'Sending {emoji} reaction from {len(valid_entries)} account(s)...',
            'task_id': task_id
        })

    except Exception as e:
        logger.error(f"React error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/react_task_status/<task_id>')
@login_required
def react_task_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── AUTO JOIN ───────────────────────────────────────────────────────────────

@app.route('/api/auto_join', methods=['POST'])
@login_required
def auto_join():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        invite_links = [l.strip() for l in data.get('invite_links', []) if l.strip()]

        if not invite_links:
            return jsonify({'success': False, 'error': 'Enter invite links'}), 400

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _join():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            joined = 0
            failed = []
            for link in invite_links:
                try:
                    if 't.me/joinchat/' in link or 't.me/+' in link:
                        hash_part = link.split('/')[-1].lstrip('+')
                        await client(functions.messages.ImportChatInviteRequest(hash=hash_part))
                    else:
                        username = link.split('/')[-1].lstrip('@')
                        entity = await client.get_entity(username)
                        await client(functions.channels.JoinChannelRequest(channel=entity))
                    joined += 1
                    await asyncio.sleep(2)
                except UserAlreadyParticipantError:
                    joined += 1
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    failed.append({'link': link, 'error': str(e)})
            await client.disconnect()
            return joined, failed

        joined, failed = run_async(_join())
        account.last_used = datetime.utcnow()
        db.session.commit()

        log_activity(current_user.id, f"Auto-joined {joined} groups/channels ({len(failed)} failed)", 'success', account_id)
        return jsonify({'success': True, 'joined': joined, 'failed': failed})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── MEMBER SCRAPER ──────────────────────────────────────────────────────────

@app.route('/api/scrape_members', methods=['POST'])
@login_required
def scrape_members():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        group_link = data.get('group_link', '').strip()
        limit = min(int(data.get('limit', 100)), 5000)
        active_only = data.get('active_only', False)

        if not group_link:
            return jsonify({'success': False, 'error': 'Group link required'}), 400

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _scrape():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            try:
                entity = await client.get_entity(group_link)
                members = []
                async for user in client.iter_participants(entity, limit=limit):
                    if user.bot:
                        continue
                    is_online = isinstance(getattr(user, 'status', None), types.UserStatusOnline)
                    if active_only and not is_online:
                        continue
                    members.append({
                        'id': user.id,
                        'username': user.username,
                        'first_name': user.first_name or '',
                        'last_name': user.last_name or '',
                        'online': is_online,
                        'phone': user.phone
                    })
                await client.disconnect()
                return members
            except Exception as e:
                await client.disconnect()
                raise e

        members = run_async(_scrape())
        account.members_scraped += len(members)
        account.last_used = datetime.utcnow()
        db.session.commit()

        log_activity(current_user.id, f"Scraped {len(members)} members from {group_link}", 'success', account_id)
        return jsonify({'success': True, 'members': members, 'count': len(members)})

    except Exception as e:
        logger.error(f"scrape_members error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── ADD MEMBERS ─────────────────────────────────────────────────────────────

@app.route('/api/add_members', methods=['POST'])
@login_required
def add_members():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        target_group = data.get('target_group', '').strip()
        members = data.get('members', [])
        delay = max(5, int(data.get('delay', 5)))
        daily_limit = min(int(data.get('daily_limit', 50)), 200)

        if not target_group or not members:
            return jsonify({'success': False, 'error': 'Target group and members required'}), 400

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        user_id = current_user.id
        sess = account.session_string

        def run_add():
            with app.app_context():
                proxy = get_proxy_for_account(user_id, account_id)
                async def _add():
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    entity = await client.get_entity(target_group)
                    added = 0
                    failed = []
                    for member in members[:daily_limit]:
                        try:
                            username = member.get('username') if isinstance(member, dict) else member
                            if not username:
                                continue
                            username = str(username).lstrip('@')
                            user_ent = await client.get_entity(username)
                            await client(InviteToChannelRequest(channel=entity, users=[user_ent]))
                            added += 1
                            await asyncio.sleep(delay)
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            failed.append(str(e))
                    await client.disconnect()
                    return added, failed

                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    added, failed = loop.run_until_complete(_add())
                    loop.close()
                    acc = TelegramAccount.query.get(account_id)
                    if acc:
                        acc.members_added += added
                        acc.last_used = datetime.utcnow()
                        db.session.commit()
                    log_activity(user_id, f"Added {added} members to {target_group} ({len(failed)} failed)", 'success', account_id)
                except Exception as e:
                    logger.error(f"add_members thread error: {e}")

        threading.Thread(target=run_add, daemon=True).start()
        return jsonify({'success': True, 'message': f'Started adding {min(len(members), daily_limit)} members. Running in background.'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── BULK DM ─────────────────────────────────────────────────────────────────

@app.route('/api/send_dm', methods=['POST'])
@login_required
def send_dm():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        usernames = data.get('usernames', [])
        message = data.get('message', '').strip()
        delay = max(3, int(data.get('delay', 5)))

        if not usernames or not message:
            return jsonify({'success': False, 'error': 'Usernames and message required'}), 400

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        user_id = current_user.id
        sess = account.session_string

        def run_dm():
            with app.app_context():
                proxy = get_proxy_for_account(user_id, account_id)
                async def _dm():
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    sent = 0
                    failed = []
                    for uname in usernames:
                        try:
                            uname = str(uname).strip().lstrip('@')
                            if not uname:
                                continue
                            await client.send_message(uname, message)
                            sent += 1
                            await asyncio.sleep(delay)
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            failed.append({'username': uname, 'error': str(e)})
                    await client.disconnect()
                    return sent, failed

                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    sent, failed = loop.run_until_complete(_dm())
                    loop.close()
                    acc = TelegramAccount.query.get(account_id)
                    if acc:
                        acc.messages_sent_today += sent
                        acc.total_messages += sent
                        acc.last_used = datetime.utcnow()
                        db.session.commit()
                    log_activity(user_id, f"Sent {sent} DMs ({len(failed)} failed)", 'success', account_id)
                except Exception as e:
                    logger.error(f"send_dm thread error: {e}")

        threading.Thread(target=run_dm, daemon=True).start()
        return jsonify({'success': True, 'message': f'Started sending DMs to {len(usernames)} users. Running in background.'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── DM CAMPAIGN (Targeted) ──────────────────────────────────────────────────

@app.route('/api/dm_campaign', methods=['POST'])
@login_required
def dm_campaign():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        targets = data.get('targets', [])
        messages = data.get('messages', [])
        delay = max(3, int(data.get('delay', 5)))
        personalize = data.get('personalize', False)

        if not targets:
            return jsonify({'success': False, 'error': 'No targets provided'}), 400
        if not messages:
            return jsonify({'success': False, 'error': 'No messages provided'}), 400

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {'status': 'running', 'sent': 0, 'failed': 0, 'total': len(targets), 'errors': []}
        user_id = current_user.id
        sess = account.session_string

        def run_campaign():
            with app.app_context():
                proxy = get_proxy_for_account(user_id, account_id)
                async def _campaign():
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    sent = 0
                    failed = 0
                    for i, target in enumerate(targets):
                        try:
                            username = str(target.get('username', '') if isinstance(target, dict) else target).strip().lstrip('@')
                            if not username:
                                continue
                            msg = messages[i % len(messages)]
                            if personalize and isinstance(target, dict):
                                first_name = target.get('first_name', 'there')
                                msg = msg.replace('{name}', first_name).replace('{first_name}', first_name)
                            await client.send_message(username, msg)
                            sent += 1
                            task_progress[task_id]['sent'] = sent
                            await asyncio.sleep(delay)
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            failed += 1
                            task_progress[task_id]['failed'] = failed
                            task_progress[task_id]['errors'].append(str(e))
                    await client.disconnect()
                    return sent, failed

                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    sent, failed = loop.run_until_complete(_campaign())
                    loop.close()
                    acc = TelegramAccount.query.get(account_id)
                    if acc:
                        acc.messages_sent_today += sent
                        acc.total_messages += sent
                        acc.last_used = datetime.utcnow()
                        db.session.commit()
                    task_progress[task_id]['status'] = 'completed'
                    log_activity(user_id, f"DM Campaign: {sent} sent, {failed} failed", 'success', account_id)
                except Exception as e:
                    task_progress[task_id]['status'] = 'failed'
                    logger.error(f"dm_campaign thread error: {e}")

        threading.Thread(target=run_campaign, daemon=True).start()
        return jsonify({'success': True, 'message': f'Campaign started for {len(targets)} targets', 'task_id': task_id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/campaign_status/<task_id>')
@login_required
def campaign_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── SUBSCRIPTION ────────────────────────────────────────────────────────────

@app.route('/api/subscription_info')
@login_required
def subscription_info():
    sub = get_user_subscription(current_user.id)
    plan = sub['plan']
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])
    account_count = TelegramAccount.query.filter_by(user_id=current_user.id).count()
    return jsonify({
        'success': True,
        'plan': plan,
        'label': limits['label'],
        'color': limits['color'],
        'price': limits['price'],
        'expires_at': sub['expires_at'].strftime('%Y-%m-%d') if sub['expires_at'] else None,
        'expired': sub['expired'],
        'limits': limits,
        'account_count': account_count,
        'is_admin': current_user.username in ADMIN_USERS,
        'plans': PLAN_LIMITS,
    })


@app.route('/api/redeem_key', methods=['POST'])
@login_required
def redeem_key():
    try:
        data = request.get_json()
        key_str = (data.get('key') or '').strip().upper()
        if not key_str:
            return jsonify({'success': False, 'error': 'Enter a license key'})

        lk = LicenseKey.query.filter_by(key=key_str).first()
        if not lk:
            return jsonify({'success': False, 'error': 'Invalid license key'})
        if not lk.is_active:
            return jsonify({'success': False, 'error': 'This key has been deactivated'})
        if lk.use_count >= lk.max_uses:
            return jsonify({'success': False, 'error': 'This key has already been used'})

        sub = Subscription.query.filter_by(user_id=current_user.id).first()
        now = datetime.utcnow()
        if not sub:
            sub = Subscription(user_id=current_user.id)
            db.session.add(sub)

        sub.plan = lk.plan
        if lk.duration_days > 0:
            current_expiry = sub.expires_at if (sub.expires_at and sub.expires_at > now) else now
            sub.expires_at = current_expiry + timedelta(days=lk.duration_days)
        else:
            sub.expires_at = None  # unlimited
        sub.updated_at = now

        lk.use_count += 1
        lk.used_by_id = current_user.id
        lk.used_at = now
        if lk.use_count >= lk.max_uses:
            lk.is_active = False

        db.session.commit()
        exp_str = sub.expires_at.strftime('%Y-%m-%d') if sub.expires_at else 'Never'
        return jsonify({'success': True, 'message': f'✅ {lk.plan.capitalize()} plan activated! Expires: {exp_str}'})
    except Exception as e:
        logger.error(f"redeem_key error: {e}")
        return jsonify({'success': False, 'error': str(e)})


# ─── CONTACT ──────────────────────────────────────────────────────────────────

@app.route('/contact')
def contact():
    return render_template('contact.html')


# ─── PAYMENT METHODS ──────────────────────────────────────────────────────────

@app.route('/api/payment_methods')
def get_payment_methods():
    methods = PaymentMethod.query.filter_by(is_active=True).order_by(PaymentMethod.display_order).all()
    return jsonify({'success': True, 'methods': [
        {'id': m.id, 'currency': m.currency, 'network': m.network, 'address': m.address}
        for m in methods
    ]})


@app.route('/api/submit_payment', methods=['POST'])
@login_required
def submit_payment():
    try:
        data = request.get_json()
        plan = data.get('plan', '').strip()
        duration_days = int(data.get('duration_days', 30))
        currency = data.get('currency', '').strip()
        txid = (data.get('txid') or '').strip()

        if plan not in PLAN_LIMITS:
            return jsonify({'success': False, 'error': 'Invalid plan'})
        if not currency:
            return jsonify({'success': False, 'error': 'Select a payment currency'})
        if not txid:
            return jsonify({'success': False, 'error': 'Transaction ID is required'})
        if len(txid) < 8:
            return jsonify({'success': False, 'error': 'Invalid transaction ID'})

        existing = PaymentRequest.query.filter_by(txid=txid).first()
        if existing:
            return jsonify({'success': False, 'error': 'This transaction ID has already been submitted'})

        amount = PLAN_LIMITS[plan]['price_usdt'] * (duration_days / 30)
        pr = PaymentRequest(
            user_id=current_user.id,
            plan=plan,
            duration_days=duration_days,
            amount_usd=amount,
            currency=currency,
            txid=txid,
            status='pending'
        )
        db.session.add(pr)
        db.session.commit()
        log_activity(current_user.id, f"Payment submitted: {plan} plan via {currency}, TxID: {txid[:20]}...", 'success')
        return jsonify({'success': True, 'message': 'Payment submitted! The admin will review and activate your plan within 24 hours.'})
    except Exception as e:
        logger.error(f"submit_payment error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/my_payments')
@login_required
def my_payments():
    reqs = PaymentRequest.query.filter_by(user_id=current_user.id).order_by(PaymentRequest.submitted_at.desc()).all()
    return jsonify({'success': True, 'payments': [
        {
            'id': r.id,
            'plan': r.plan,
            'duration_days': r.duration_days,
            'amount_usd': r.amount_usd,
            'currency': r.currency,
            'txid': r.txid,
            'status': r.status,
            'notes': r.notes,
            'submitted_at': r.submitted_at.strftime('%Y-%m-%d %H:%M'),
        } for r in reqs
    ]})


# ─── REPORT TEMPLATE GENERATOR ────────────────────────────────────────────────

@app.route('/api/report_template', methods=['POST'])
@login_required
def report_template():
    data = request.get_json()
    report_type = data.get('type', 'spam')
    custom_reason = (data.get('reason') or '').strip()

    templates = {
        'spam': [
            "This account is sending unsolicited spam messages to multiple users and groups.",
            "Repeated spam activity detected from this user. They are flooding channels with unwanted promotional content.",
            "This user is mass-spamming groups with advertisements and irrelevant content.",
            "Reporting for systematic spam behavior: the account posts identical messages across many groups.",
        ],
        'scam': [
            "This account is running a cryptocurrency scam, promising unrealistic returns to trick users.",
            "Fraudulent activity: this user is impersonating a legitimate service to steal funds.",
            "This is a scam account offering fake investment opportunities and financial fraud.",
            "Reporting for scam behavior: false promises, fake testimonials, and deceptive practices.",
        ],
        'fake': [
            "This account is impersonating a real person or brand using stolen photos and content.",
            "Fake account: this profile is pretending to be someone else to deceive users.",
            "This user is running a fake profile, misrepresenting their identity for malicious purposes.",
        ],
        'violence': [
            "This account is promoting violence and sharing content that glorifies harm to individuals.",
            "Violent and threatening content: this user is posting threats and inciting violence.",
            "Reporting for violent content: the account shares graphic material and promotes harm.",
        ],
        'pornography': [
            "This account is distributing explicit adult content in violation of Telegram's terms of service.",
            "Explicit pornographic material is being shared from this account without consent.",
        ],
        'copyright': [
            "This account is distributing copyrighted material without authorization from the rights holder.",
            "Copyright violation: the user is sharing protected content, movies, software, or music illegally.",
        ],
        'child_abuse': [
            "This account contains material that exploits minors. Immediate action is required.",
            "Child exploitation content detected. This account must be reviewed and removed immediately.",
        ],
    }

    if custom_reason:
        generated = [
            f"Reporting this account for the following reason: {custom_reason}.",
            f"This user is violating Telegram's terms of service. Reason: {custom_reason}.",
            f"Please review and take action on this account. Issue: {custom_reason}.",
            f"Community report: {custom_reason}. This account should be investigated and removed.",
        ]
        return jsonify({'success': True, 'templates': generated, 'type': 'custom'})

    return jsonify({'success': True, 'templates': templates.get(report_type, templates['spam']), 'type': report_type})


# ─── ADMIN ────────────────────────────────────────────────────────────────────

def admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or current_user.username not in ADMIN_USERS:
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated


@app.route('/admin')
@login_required
def admin_panel():
    if current_user.username not in ADMIN_USERS:
        return redirect(url_for('dashboard'))
    return render_template('admin.html', user=current_user)


@app.route('/api/admin/users')
@login_required
@admin_required
def admin_users():
    users = User.query.order_by(User.created_at.desc()).all()
    result = []
    for u in users:
        sub = get_user_subscription(u.id)
        ac_count = TelegramAccount.query.filter_by(user_id=u.id).count()
        result.append({
            'id': u.id,
            'username': u.username,
            'created_at': u.created_at.strftime('%Y-%m-%d %H:%M'),
            'plan': sub['plan'],
            'expires_at': sub['expires_at'].strftime('%Y-%m-%d') if sub['expires_at'] else '–',
            'expired': sub['expired'],
            'accounts': ac_count,
            'is_banned': getattr(u, 'is_banned', False) or False,
            'ban_reason': getattr(u, 'ban_reason', '') or '',
        })
    return jsonify({'success': True, 'users': result})


@app.route('/api/admin/set_plan', methods=['POST'])
@login_required
@admin_required
def admin_set_plan():
    try:
        data = request.get_json()
        user_id = int(data.get('user_id'))
        plan = data.get('plan', 'free')
        days = int(data.get('days', 30))

        if plan not in PLAN_LIMITS:
            return jsonify({'success': False, 'error': 'Invalid plan'})

        sub = Subscription.query.filter_by(user_id=user_id).first()
        if not sub:
            sub = Subscription(user_id=user_id)
            db.session.add(sub)

        now = datetime.utcnow()
        sub.plan = plan
        sub.updated_at = now
        if plan == 'unlimited' or days == 0:
            sub.expires_at = None
        else:
            sub.expires_at = now + timedelta(days=days)

        db.session.commit()
        return jsonify({'success': True, 'message': f'Plan set to {plan}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/generate_keys', methods=['POST'])
@login_required
@admin_required
def admin_generate_keys():
    try:
        data = request.get_json()
        count = min(int(data.get('count', 1)), 100)
        plan = data.get('plan', 'basic')
        days = int(data.get('days', 30))
        max_uses = int(data.get('max_uses', 1))
        notes = data.get('notes', '')

        if plan not in PLAN_LIMITS:
            return jsonify({'success': False, 'error': 'Invalid plan'})

        keys = []
        for _ in range(count):
            while True:
                key = generate_license_key()
                if not LicenseKey.query.filter_by(key=key).first():
                    break
            lk = LicenseKey(
                key=key, plan=plan, duration_days=days,
                max_uses=max_uses, notes=notes
            )
            db.session.add(lk)
            keys.append(key)

        db.session.commit()
        return jsonify({'success': True, 'keys': keys, 'count': len(keys)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/keys')
@login_required
@admin_required
def admin_keys():
    keys = LicenseKey.query.order_by(LicenseKey.created_at.desc()).limit(200).all()
    result = []
    for k in keys:
        used_by_name = None
        if k.used_by_id:
            u = User.query.get(k.used_by_id)
            used_by_name = u.username if u else f'User#{k.used_by_id}'
        result.append({
            'id': k.id,
            'key': k.key,
            'plan': k.plan,
            'duration_days': k.duration_days,
            'max_uses': k.max_uses,
            'use_count': k.use_count,
            'is_active': k.is_active,
            'used_by': used_by_name,
            'used_at': k.used_at.strftime('%Y-%m-%d') if k.used_at else None,
            'created_at': k.created_at.strftime('%Y-%m-%d'),
            'notes': k.notes or '',
        })
    return jsonify({'success': True, 'keys': result})


@app.route('/api/admin/revoke_key/<int:key_id>', methods=['POST'])
@login_required
@admin_required
def admin_revoke_key(key_id):
    lk = LicenseKey.query.get_or_404(key_id)
    lk.is_active = not lk.is_active
    db.session.commit()
    return jsonify({'success': True, 'active': lk.is_active})


@app.route('/api/admin/delete_user/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_delete_user(user_id):
    if user_id == current_user.id:
        return jsonify({'success': False, 'error': 'Cannot delete yourself'})
    user = User.query.get_or_404(user_id)
    db.session.delete(user)
    db.session.commit()
    return jsonify({'success': True})


@app.route('/api/admin/stats')
@login_required
@admin_required
def admin_stats():
    total_users = User.query.count()
    total_accounts = TelegramAccount.query.count()
    total_keys = LicenseKey.query.count()
    used_keys = LicenseKey.query.filter(LicenseKey.use_count > 0).count()
    plan_counts = {}
    for plan in PLAN_LIMITS:
        subs = Subscription.query.filter_by(plan=plan).count()
        plan_counts[plan] = subs
    plan_counts['free'] = total_users - sum(v for k, v in plan_counts.items() if k != 'free')
    pending_payments = PaymentRequest.query.filter_by(status='pending').count()
    return jsonify({
        'success': True,
        'total_users': total_users,
        'total_accounts': total_accounts,
        'total_keys': total_keys,
        'used_keys': used_keys,
        'plan_counts': plan_counts,
        'pending_payments': pending_payments,
    })


# ─── ADMIN PAYMENT MANAGEMENT ────────────────────────────────────────────────

@app.route('/api/admin/pending_payments')
@login_required
@admin_required
def admin_pending_payments():
    reqs = PaymentRequest.query.order_by(PaymentRequest.submitted_at.desc()).limit(200).all()
    result = []
    for r in reqs:
        u = User.query.get(r.user_id)
        result.append({
            'id': r.id,
            'user_id': r.user_id,
            'username': u.username if u else f'User#{r.user_id}',
            'plan': r.plan,
            'duration_days': r.duration_days,
            'amount_usd': r.amount_usd,
            'currency': r.currency,
            'txid': r.txid,
            'status': r.status,
            'notes': r.notes or '',
            'submitted_at': r.submitted_at.strftime('%Y-%m-%d %H:%M'),
            'reviewed_at': r.reviewed_at.strftime('%Y-%m-%d %H:%M') if r.reviewed_at else None,
        })
    return jsonify({'success': True, 'payments': result})


@app.route('/api/admin/review_payment/<int:pay_id>', methods=['POST'])
@login_required
@admin_required
def admin_review_payment(pay_id):
    try:
        data = request.get_json()
        action = data.get('action', 'approve')  # approve or reject
        notes = (data.get('notes') or '').strip()

        pr = PaymentRequest.query.get_or_404(pay_id)
        pr.status = 'approved' if action == 'approve' else 'rejected'
        pr.notes = notes
        pr.reviewed_at = datetime.utcnow()
        pr.reviewed_by = current_user.username

        if action == 'approve':
            sub = Subscription.query.filter_by(user_id=pr.user_id).first()
            now = datetime.utcnow()
            if not sub:
                sub = Subscription(user_id=pr.user_id)
                db.session.add(sub)
            sub.plan = pr.plan
            sub.updated_at = now
            if pr.duration_days <= 0 or pr.plan == 'unlimited':
                sub.expires_at = None
            else:
                base = sub.expires_at if (sub.expires_at and sub.expires_at > now) else now
                sub.expires_at = base + timedelta(days=pr.duration_days)
            log_activity(pr.user_id, f"Plan upgraded to {pr.plan} by admin (payment approved)", 'success')

        db.session.commit()
        return jsonify({'success': True, 'message': f'Payment {pr.status}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/payment_methods', methods=['GET'])
@login_required
@admin_required
def admin_get_payment_methods():
    methods = PaymentMethod.query.order_by(PaymentMethod.display_order).all()
    return jsonify({'success': True, 'methods': [
        {'id': m.id, 'currency': m.currency, 'network': m.network,
         'address': m.address, 'is_active': m.is_active, 'display_order': m.display_order}
        for m in methods
    ]})


@app.route('/api/admin/payment_methods', methods=['POST'])
@login_required
@admin_required
def admin_save_payment_methods():
    try:
        data = request.get_json()
        methods = data.get('methods', [])
        PaymentMethod.query.delete()
        for i, m in enumerate(methods):
            pm = PaymentMethod(
                currency=m.get('currency', ''),
                network=m.get('network', ''),
                address=m.get('address', ''),
                is_active=bool(m.get('is_active', True)),
                display_order=i
            )
            db.session.add(pm)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Payment methods saved'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── ADMIN ALL-ACCOUNTS VIEW ──────────────────────────────────────────────────

@app.route('/api/admin/all_accounts')
@login_required
@admin_required
def admin_all_accounts():
    accounts = TelegramAccount.query.order_by(TelegramAccount.added_at.desc()).all()
    result = []
    for a in accounts:
        owner = User.query.get(a.user_id)
        result.append({
            'id': a.id,
            'user_id': a.user_id,
            'owner': owner.username if owner else f'User#{a.user_id}',
            'phone': a.phone,
            'is_active': a.is_active,
            'connected': bool(a.session_string and a.session_string not in ('demo', 'active')),
            'messages_sent': a.total_messages,
            'reports_sent': a.total_reports,
            'members_added': a.members_added,
            'connected_at': a.added_at.strftime('%Y-%m-%d %H:%M') if a.added_at else '—',
        })
    return jsonify({'success': True, 'accounts': result})


@app.route('/api/admin/toggle_account/<int:account_id>', methods=['POST'])
@login_required
@admin_required
def admin_toggle_account(account_id):
    account = TelegramAccount.query.get_or_404(account_id)
    account.is_active = not account.is_active
    db.session.commit()
    return jsonify({'success': True, 'is_active': account.is_active})


@app.route('/api/admin/delete_account/<int:account_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_delete_account(account_id):
    account = TelegramAccount.query.get_or_404(account_id)
    db.session.delete(account)
    db.session.commit()
    return jsonify({'success': True})


@app.route('/api/admin/use_account/<int:account_id>', methods=['GET'])
@login_required
@admin_required
def admin_use_account(account_id):
    """Returns the account details so admin can use it in operations."""
    account = TelegramAccount.query.get_or_404(account_id)
    owner = User.query.get(account.user_id)
    owner_settings = Settings.query.filter_by(user_id=account.user_id).first()
    return jsonify({
        'success': True,
        'account': {
            'id': account.id,
            'phone': account.phone,
            'owner': owner.username if owner else f'User#{account.user_id}',
            'connected': bool(account.session_string and account.session_string not in ('demo', 'active')),
        },
        'has_credentials': bool(owner_settings and owner_settings.api_id)
    })


# ─── NEW TELEGRAM FEATURES ────────────────────────────────────────────────────

@app.route('/api/check_username', methods=['POST'])
@login_required
def check_username():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        username = (data.get('username') or '').strip().lstrip('@')
        if not username:
            return jsonify({'success': False, 'error': 'Enter a username'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _check():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                result = await client(functions.account.CheckUsernameRequest(username=username))
                return result
            finally:
                await client.disconnect()

        available = run_async(_check())
        return jsonify({'success': True, 'username': username, 'available': bool(available),
                        'message': f'@{username} is {"✅ available!" if available else "❌ taken"}'})
    except Exception as e:
        logger.error(f"check_username error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/send_saved_message', methods=['POST'])
@login_required
def send_saved_message():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        message = (data.get('message') or '').strip()
        if not message:
            return jsonify({'success': False, 'error': 'Message required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _send():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                await client.send_message('me', message)
            finally:
                await client.disconnect()

        run_async(_send())
        log_activity(current_user.id, f"Sent message to Saved Messages", 'success', account_id)
        return jsonify({'success': True, 'message': 'Sent to Saved Messages ✅'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/delete_my_messages', methods=['POST'])
@login_required
def delete_my_messages():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        chat = (data.get('chat') or '').strip()
        count = min(int(data.get('count', 10)), 100)
        if not chat:
            return jsonify({'success': False, 'error': 'Chat/group required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _delete():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                me = await client.get_me()
                entity = await client.get_entity(chat)
                msg_ids = []
                async for msg in client.iter_messages(entity, limit=count * 3, from_user=me.id):
                    msg_ids.append(msg.id)
                    if len(msg_ids) >= count:
                        break
                if msg_ids:
                    await client.delete_messages(entity, msg_ids)
                return len(msg_ids)
            finally:
                await client.disconnect()

        deleted = run_async(_delete())
        log_activity(current_user.id, f"Deleted {deleted} messages from {chat}", 'success', account_id)
        return jsonify({'success': True, 'message': f'Deleted {deleted} messages ✅'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/get_group_info', methods=['POST'])
@login_required
def get_group_info():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id'))
        group = (data.get('group') or '').strip()
        if not group:
            return jsonify({'success': False, 'error': 'Group/channel required'})

        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'error': 'Unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _info():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                entity = await client.get_entity(group)
                full = await client.get_participants(entity, limit=0)
                title = getattr(entity, 'title', str(entity))
                username = getattr(entity, 'username', None)
                member_count = getattr(entity, 'participants_count', None) or full.total
                entity_id = entity.id
                is_channel = hasattr(entity, 'broadcast') and entity.broadcast
                is_megagroup = getattr(entity, 'megagroup', False)
                restricted = getattr(entity, 'restricted', False)
                verified = getattr(entity, 'verified', False)
                scam = getattr(entity, 'scam', False)
                return {
                    'title': title,
                    'username': username,
                    'id': entity_id,
                    'members': member_count,
                    'type': 'Channel' if is_channel else ('Supergroup' if is_megagroup else 'Group'),
                    'restricted': restricted,
                    'verified': verified,
                    'scam': scam,
                }
            finally:
                await client.disconnect()

        info = run_async(_info())
        return jsonify({'success': True, 'info': info})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── DASHBOARD STATS ─────────────────────────────────────────────────────────

@app.route('/api/dashboard_stats')
@login_required
def dashboard_stats():
    try:
        admin_mode = is_admin()
        if admin_mode:
            accounts = TelegramAccount.query.order_by(TelegramAccount.added_at.desc()).all()
        else:
            accounts = TelegramAccount.query.filter_by(user_id=current_user.id).all()
        logs = ActivityLog.query.filter_by(user_id=current_user.id).order_by(ActivityLog.timestamp.desc()).limit(50).all()
        scheduled = ScheduledBroadcast.query.filter_by(user_id=current_user.id, status='pending').count()

        sub = get_user_subscription(current_user.id)
        plan = sub['plan']
        limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])
        return jsonify({
            'total_accounts': len(accounts),
            'messages_today': sum(a.messages_sent_today for a in accounts),
            'reports_today': sum(a.reports_sent_today for a in accounts),
            'members_scraped': sum(a.members_scraped for a in accounts),
            'members_added': sum(a.members_added for a in accounts),
            'scheduled_broadcasts': scheduled,
            'plan': plan,
            'plan_label': limits['label'],
            'plan_color': limits['color'],
            'plan_limit_accounts': limits['accounts'],
            'is_admin': admin_mode,
            'accounts': [{
                'id': a.id,
                'phone': a.phone,
                'is_active': a.is_active,
                'connected': bool(a.session_string and a.session_string not in ('demo', 'active')),
                'messages_sent': a.messages_sent_today,
                'reports_sent': a.reports_sent_today,
                'members_scraped': a.members_scraped,
                'members_added': a.members_added,
                'total_messages': a.total_messages,
                'total_reports': a.total_reports,
                'owner': User.query.get(a.user_id).username if admin_mode else current_user.username,
            } for a in accounts],
            'logs': [{
                'action': log.action,
                'status': log.status,
                'timestamp': log.timestamp.isoformat()
            } for log in logs]
        })
    except Exception as e:
        logger.error(f"dashboard_stats error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/reset_daily_stats', methods=['POST'])
@login_required
def reset_daily_stats():
    try:
        accounts = TelegramAccount.query.filter_by(user_id=current_user.id).all()
        for a in accounts:
            a.messages_sent_today = 0
            a.reports_sent_today = 0
        db.session.commit()
        log_activity(current_user.id, "Reset daily statistics", 'success')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── BACKGROUND THREADS ──────────────────────────────────────────────────────

def schedule_daily_reset():
    def reset():
        while True:
            now = datetime.now()
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time.sleep((next_midnight - now).total_seconds())
            with app.app_context():
                for a in TelegramAccount.query.all():
                    a.messages_sent_today = 0
                    a.reports_sent_today = 0
                db.session.commit()
    threading.Thread(target=reset, daemon=True).start()


def run_scheduled_broadcasts():
    def checker():
        while True:
            time.sleep(30)
            try:
                with app.app_context():
                    now = datetime.utcnow()
                    pending = ScheduledBroadcast.query.filter(
                        ScheduledBroadcast.status == 'pending',
                        ScheduledBroadcast.scheduled_at <= now
                    ).all()

                    for sb in pending:
                        sb.status = 'running'
                        db.session.commit()
                        sb_id = sb.id
                        account_id = sb.account_id
                        user_id = sb.user_id

                        acc = TelegramAccount.query.get(account_id)
                        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
                            sb.status = 'failed'
                            db.session.commit()
                            continue

                        api_id, api_hash = get_api_credentials(user_id)
                        if not api_id:
                            sb.status = 'failed'
                            db.session.commit()
                            continue

                        group_ids = json.loads(sb.groups)
                        content = sb.content
                        caption = sb.caption or ''
                        message_type = sb.message_type
                        delay = sb.delay
                        sess = acc.session_string

                        def run_sb(sb_id=sb_id, account_id=account_id, user_id=user_id,
                                   group_ids=group_ids, content=content, caption=caption,
                                   message_type=message_type, delay=delay, sess=sess,
                                   api_id=api_id, api_hash=api_hash):
                            with app.app_context():
                                proxy = get_proxy_for_account(user_id, account_id)
                                async def _sb():
                                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                                    await client.connect()
                                    sent = 0
                                    for gid in group_ids:
                                        try:
                                            entity = await client.get_entity(int(gid))
                                            if message_type == 'text':
                                                await client.send_message(entity, content)
                                            else:
                                                await client.send_file(entity, content, caption=caption or content)
                                            sent += 1
                                            await asyncio.sleep(delay)
                                        except FloodWaitError as e:
                                            await asyncio.sleep(e.seconds)
                                        except Exception:
                                            pass
                                    await client.disconnect()
                                    return sent

                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                try:
                                    sent = loop.run_until_complete(_sb())
                                    s = ScheduledBroadcast.query.get(sb_id)
                                    acc2 = TelegramAccount.query.get(account_id)
                                    if s:
                                        interval = s.repeat_interval_minutes
                                        if interval and interval > 0:
                                            s.status = 'pending'
                                            s.run_count = (s.run_count or 0) + 1
                                            s.next_run_at = datetime.utcnow() + timedelta(minutes=interval)
                                            s.scheduled_at = s.next_run_at
                                            s.sent_count = (s.sent_count or 0) + sent
                                        else:
                                            s.status = 'completed'
                                            s.sent_count = sent
                                            s.completed_at = datetime.utcnow()
                                    if acc2:
                                        acc2.messages_sent_today += sent
                                        acc2.total_messages += sent
                                    db.session.commit()
                                    log_activity(user_id, f"Scheduled broadcast executed: {sent} sent", 'success', account_id)
                                except Exception as e:
                                    s = ScheduledBroadcast.query.get(sb_id)
                                    if s:
                                        s.status = 'failed'
                                        db.session.commit()
                                    logger.error(f"Scheduled broadcast failed: {e}")
                                finally:
                                    loop.close()

                        threading.Thread(target=run_sb, daemon=True).start()
            except Exception as e:
                logger.error(f"Scheduled broadcast checker error: {e}")

    threading.Thread(target=checker, daemon=True).start()


# ─── MULTI-ACCOUNT ADD MEMBERS ───────────────────────────────────────────────

@app.route('/api/add_members_multi', methods=['POST'])
@login_required
def add_members_multi():
    """Add members using multiple accounts concurrently with live progress tracking."""
    try:
        data = request.get_json()
        account_ids = [int(x) for x in data.get('account_ids', [])]
        target_group = data.get('target_group', '').strip()
        members = data.get('members', [])
        delay = max(5, int(data.get('delay', 5)))
        daily_limit_per_account = min(int(data.get('daily_limit', 50)), 200)
        distribute = data.get('distribute', True)

        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'}), 400
        if not target_group:
            return jsonify({'success': False, 'error': 'Target group required'}), 400
        if not members:
            return jsonify({'success': False, 'error': 'Member list is empty'}), 400

        valid_accounts = []
        for aid in account_ids:
            acc = TelegramAccount.query.get(aid)
            if acc and can_access_account(acc) and acc.session_string and acc.session_string not in ('demo', 'active'):
                valid_accounts.append(acc)

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts selected'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        task_id = str(uuid.uuid4())
        per_account = {}
        if distribute:
            chunk = max(1, len(members) // len(valid_accounts))
            for i, acc in enumerate(valid_accounts):
                start = i * chunk
                end = start + chunk if i < len(valid_accounts) - 1 else len(members)
                per_account[acc.id] = members[start:end]
        else:
            for acc in valid_accounts:
                per_account[acc.id] = members[:daily_limit_per_account]

        task_progress[task_id] = {
            'status': 'running',
            'total_added': 0,
            'total_failed': 0,
            'total_members': len(members),
            'accounts': {str(acc.id): {'phone': acc.phone, 'added': 0, 'failed': 0, 'status': 'waiting', 'errors': []}
                         for acc in valid_accounts}
        }

        user_id = current_user.id

        def run_account(acc, m_list):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)
                task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'running'

                async def _add():
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    added = 0
                    failed = 0
                    errors = []
                    try:
                        entity = await client.get_entity(target_group)
                        for member in m_list[:daily_limit_per_account]:
                            try:
                                username = member.get('username') if isinstance(member, dict) else str(member)
                                if not username:
                                    continue
                                username = username.lstrip('@')
                                user_ent = await client.get_entity(username)
                                await client(InviteToChannelRequest(channel=entity, users=[user_ent]))
                                added += 1
                                task_progress[task_id]['accounts'][str(acc.id)]['added'] = added
                                task_progress[task_id]['total_added'] = sum(
                                    v['added'] for v in task_progress[task_id]['accounts'].values()
                                )
                                await asyncio.sleep(delay)
                            except FloodWaitError as e:
                                await asyncio.sleep(e.seconds)
                            except Exception as e:
                                failed += 1
                                errors.append(str(e)[:80])
                                task_progress[task_id]['accounts'][str(acc.id)]['failed'] = failed
                                task_progress[task_id]['total_failed'] = sum(
                                    v['failed'] for v in task_progress[task_id]['accounts'].values()
                                )
                    finally:
                        await client.disconnect()
                    return added, failed, errors

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    added, failed, errors = loop.run_until_complete(_add())
                    a = TelegramAccount.query.get(acc.id)
                    if a:
                        a.members_added += added
                        a.last_used = datetime.utcnow()
                        db.session.commit()
                    task_progress[task_id]['accounts'][str(acc.id)].update({
                        'status': 'done', 'added': added, 'failed': failed, 'errors': errors
                    })
                    log_activity(user_id, f"[Multi] Added {added} members to {target_group} via {acc.phone}", 'success', acc.id)
                except Exception as e:
                    task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'failed'
                    task_progress[task_id]['accounts'][str(acc.id)]['errors'].append(str(e))
                    logger.error(f"add_members_multi account {acc.phone} error: {e}")
                finally:
                    loop.close()

            # check if all done
            all_done = all(v['status'] in ('done', 'failed')
                           for v in task_progress[task_id]['accounts'].values())
            if all_done:
                task_progress[task_id]['status'] = 'completed'

        for acc in valid_accounts:
            threading.Thread(target=run_account, args=(acc, per_account[acc.id]), daemon=True).start()

        return jsonify({'success': True, 'task_id': task_id,
                        'message': f'Started adding members using {len(valid_accounts)} accounts'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/add_members_multi_status/<task_id>')
@login_required
def add_members_multi_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── ALL-ACCOUNTS AUTO JOIN ───────────────────────────────────────────────────

@app.route('/api/auto_join_all', methods=['POST'])
@login_required
def auto_join_all():
    """Join groups/channels with ALL connected accounts simultaneously."""
    try:
        data = request.get_json()
        invite_links = [l.strip() for l in data.get('invite_links', []) if l.strip()]
        account_ids = data.get('account_ids', [])

        if not invite_links:
            return jsonify({'success': False, 'error': 'Enter at least one invite link'}), 400

        valid_accounts = []
        if account_ids:
            for aid in account_ids:
                acc = TelegramAccount.query.get(int(aid))
                if acc and can_access_account(acc) and acc.session_string and acc.session_string not in ('demo', 'active'):
                    valid_accounts.append(acc)
        else:
            all_accs = TelegramAccount.query.filter_by(user_id=current_user.id, is_active=True).all()
            valid_accounts = [a for a in all_accs if a.session_string and a.session_string not in ('demo', 'active')]

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts available'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {
            'status': 'running',
            'total_joined': 0,
            'total_accounts': len(valid_accounts),
            'completed_accounts': 0,
            'accounts': {str(acc.id): {'phone': acc.phone, 'joined': 0, 'failed': [], 'status': 'waiting'}
                         for acc in valid_accounts}
        }

        user_id = current_user.id

        def join_with_account(acc):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)
                task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'running'

                async def _join():
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    joined = 0
                    failed = []
                    for link in invite_links:
                        try:
                            if 't.me/joinchat/' in link or 't.me/+' in link:
                                hash_part = link.split('/')[-1].lstrip('+')
                                await client(functions.messages.ImportChatInviteRequest(hash=hash_part))
                            else:
                                username = link.split('/')[-1].lstrip('@')
                                entity = await client.get_entity(username)
                                await client(functions.channels.JoinChannelRequest(channel=entity))
                            joined += 1
                            await asyncio.sleep(2)
                        except UserAlreadyParticipantError:
                            joined += 1
                        except FloodWaitError as e:
                            await asyncio.sleep(min(e.seconds, 30))
                        except Exception as e:
                            failed.append({'link': link, 'error': str(e)[:80]})
                    await client.disconnect()
                    return joined, failed

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    joined, failed = loop.run_until_complete(_join())
                    a = TelegramAccount.query.get(acc.id)
                    if a:
                        a.last_used = datetime.utcnow()
                        db.session.commit()
                    task_progress[task_id]['accounts'][str(acc.id)].update({
                        'status': 'done', 'joined': joined, 'failed': failed
                    })
                    task_progress[task_id]['total_joined'] += joined
                    log_activity(user_id, f"[All-Join] {acc.phone} joined {joined}/{len(invite_links)} groups", 'success', acc.id)
                except Exception as e:
                    task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'failed'
                    task_progress[task_id]['accounts'][str(acc.id)]['failed'].append({'error': str(e)})
                    logger.error(f"auto_join_all account {acc.phone} error: {e}")
                finally:
                    loop.close()
                    task_progress[task_id]['completed_accounts'] += 1
                    if task_progress[task_id]['completed_accounts'] >= len(valid_accounts):
                        task_progress[task_id]['status'] = 'completed'

        for acc in valid_accounts:
            threading.Thread(target=join_with_account, args=(acc,), daemon=True).start()

        return jsonify({'success': True, 'task_id': task_id,
                        'message': f'All {len(valid_accounts)} accounts are now joining the group(s)'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/auto_join_all_status/<task_id>')
@login_required
def auto_join_all_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── ADMIN: BAN / UNBAN / LOGS ───────────────────────────────────────────────

@app.route('/api/admin/ban_user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def admin_ban_user(user_id):
    try:
        data = request.get_json() or {}
        reason = (data.get('reason') or 'Banned by admin').strip()
        user = User.query.get_or_404(user_id)
        if user.username in ADMIN_USERS:
            return jsonify({'success': False, 'error': 'Cannot ban admin accounts'})
        user.is_banned = True
        user.ban_reason = reason
        db.session.commit()
        log_activity(current_user.id, f"Banned user {user.username}: {reason}", 'warning')
        return jsonify({'success': True, 'message': f'User {user.username} has been banned'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/unban_user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def admin_unban_user(user_id):
    try:
        user = User.query.get_or_404(user_id)
        user.is_banned = False
        user.ban_reason = None
        db.session.commit()
        log_activity(current_user.id, f"Unbanned user {user.username}", 'success')
        return jsonify({'success': True, 'message': f'User {user.username} has been unbanned'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/activity_logs')
@login_required
@admin_required
def admin_activity_logs():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        user_filter = request.args.get('user_id')
        q = ActivityLog.query
        if user_filter:
            q = q.filter_by(user_id=int(user_filter))
        logs = q.order_by(ActivityLog.timestamp.desc()).limit(per_page).offset((page-1)*per_page).all()
        return jsonify({'success': True, 'logs': [{
            'id': l.id,
            'user': User.query.get(l.user_id).username if User.query.get(l.user_id) else f'#{l.user_id}',
            'action': l.action,
            'status': l.status,
            'account_id': l.account_id,
            'timestamp': l.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        } for l in logs]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/login_history')
@login_required
@admin_required
def admin_login_history():
    try:
        logs = LoginHistory.query.order_by(LoginHistory.timestamp.desc()).limit(200).all()
        return jsonify({'success': True, 'history': [{
            'user': User.query.get(l.user_id).username if User.query.get(l.user_id) else f'#{l.user_id}',
            'ip': l.ip_address,
            'status': l.status,
            'timestamp': l.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        } for l in logs]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/live_stats')
@login_required
@admin_required
def admin_live_stats():
    try:
        total_users = User.query.count()
        banned_users = User.query.filter_by(is_banned=True).count()
        total_accounts = TelegramAccount.query.count()
        connected_accounts = sum(1 for a in TelegramAccount.query.all() if a.session_string and a.session_string not in ('demo', 'active'))
        msgs_today = db.session.query(db.func.sum(TelegramAccount.messages_sent_today)).scalar() or 0
        reports_today = db.session.query(db.func.sum(TelegramAccount.reports_sent_today)).scalar() or 0
        pending_payments = PaymentRequest.query.filter_by(status='pending').count()
        recent_logs = ActivityLog.query.order_by(ActivityLog.timestamp.desc()).limit(20).all()
        return jsonify({
            'success': True,
            'total_users': total_users,
            'banned_users': banned_users,
            'total_accounts': total_accounts,
            'connected_accounts': connected_accounts,
            'messages_today': msgs_today,
            'reports_today': reports_today,
            'pending_payments': pending_payments,
            'recent_activity': [{
                'user': User.query.get(l.user_id).username if User.query.get(l.user_id) else '?',
                'action': l.action,
                'status': l.status,
                'time': l.timestamp.strftime('%H:%M:%S'),
            } for l in recent_logs]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── LOGIN HISTORY ────────────────────────────────────────────────────────────

@app.route('/api/login_history')
@login_required
def get_login_history():
    try:
        logs = LoginHistory.query.filter_by(user_id=current_user.id).order_by(LoginHistory.timestamp.desc()).limit(50).all()
        return jsonify({'success': True, 'history': [{
            'ip': l.ip_address,
            'status': l.status,
            'timestamp': l.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        } for l in logs]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── INBOX / CHAT HISTORY ─────────────────────────────────────────────────────

@app.route('/api/get_inbox', methods=['POST'])
@login_required
def get_inbox():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id', 0))
        limit = int(data.get('limit', 30))
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'success': False, 'error': 'Access denied'})
        if not account.session_string:
            return jsonify({'success': False, 'error': 'Account not connected'})
        api_id, api_hash = get_account_api_credentials(account)
        if not api_id:
            return jsonify({'success': False, 'error': 'No API credentials configured'})
        proxy = get_proxy_for_account(account.user_id)
        async def _get():
            client = make_client(account.session_string, api_id, api_hash, proxy)
            await client.connect()
            dialogs = await client.get_dialogs(limit=limit)
            result = []
            for d in dialogs:
                unread = d.unread_count if hasattr(d, 'unread_count') else 0
                msg_text = ''
                if d.message:
                    msg_text = getattr(d.message, 'text', '') or '[media]'
                result.append({
                    'id': d.id,
                    'name': d.name or str(d.id),
                    'unread': unread,
                    'last_message': msg_text[:100],
                    'date': d.date.strftime('%Y-%m-%d %H:%M') if d.date else '',
                    'is_group': d.is_group,
                    'is_channel': d.is_channel,
                })
            await client.disconnect()
            return result
        chats = run_async(_get())
        return jsonify({'success': True, 'chats': chats})
    except Exception as e:
        logger.error(f"get_inbox error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/get_chat_messages', methods=['POST'])
@login_required
def get_chat_messages():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id', 0))
        chat_id = data.get('chat_id')
        limit = int(data.get('limit', 50))
        account = TelegramAccount.query.get_or_404(account_id)
        if not can_access_account(account):
            return jsonify({'success': False, 'error': 'Access denied'})
        if not account.session_string:
            return jsonify({'success': False, 'error': 'Account not connected'})
        api_id, api_hash = get_account_api_credentials(account)
        if not api_id:
            return jsonify({'success': False, 'error': 'No API credentials'})
        proxy = get_proxy_for_account(account.user_id)
        async def _get():
            client = make_client(account.session_string, api_id, api_hash, proxy)
            await client.connect()
            messages = await client.get_messages(int(chat_id), limit=limit)
            result = []
            for m in messages:
                result.append({
                    'id': m.id,
                    'text': m.text or '[media]',
                    'from_id': str(m.from_id) if m.from_id else 'unknown',
                    'is_outgoing': m.out,
                    'date': m.date.strftime('%Y-%m-%d %H:%M:%S') if m.date else '',
                    'reply_to': m.reply_to_msg_id,
                })
            await client.disconnect()
            return result
        messages = run_async(_get())
        return jsonify({'success': True, 'messages': messages})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── ACCOUNT HEALTH CHECK ─────────────────────────────────────────────────────

@app.route('/api/account_health', methods=['POST'])
@login_required
def account_health():
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        if not account_ids:
            accs = TelegramAccount.query.filter_by(user_id=current_user.id).all() if not is_admin() else TelegramAccount.query.all()
            account_ids = [a.id for a in accs]
        results = []
        for acc_id in account_ids:
            account = TelegramAccount.query.get(acc_id)
            if not account or not can_access_account(account):
                continue
            if not account.session_string:
                results.append({'id': acc_id, 'phone': account.phone, 'status': 'not_connected', 'health': 'unknown'})
                continue
            api_id, api_hash = get_account_api_credentials(account)
            if not api_id:
                results.append({'id': acc_id, 'phone': account.phone, 'status': 'no_credentials', 'health': 'unknown'})
                continue
            proxy = get_proxy_for_account(account.user_id)
            try:
                async def _check():
                    client = make_client(account.session_string, api_id, api_hash, proxy)
                    await client.connect()
                    try:
                        me = await client.get_me()
                        await client.disconnect()
                        return {'ok': True, 'name': f"{me.first_name or ''} {me.last_name or ''}".strip(), 'username': me.username, 'premium': me.premium}
                    except Exception as ex:
                        await client.disconnect()
                        return {'ok': False, 'error': str(ex)}
                info = run_async(_check())
                if info['ok']:
                    results.append({'id': acc_id, 'phone': account.phone, 'status': 'healthy', 'health': 'good',
                                    'name': info.get('name'), 'username': info.get('username'), 'premium': info.get('premium')})
                else:
                    err = info.get('error', '').lower()
                    health = 'banned' if 'auth' in err or 'deactivated' in err or 'banned' in err else 'error'
                    results.append({'id': acc_id, 'phone': account.phone, 'status': 'error', 'health': health, 'error': info.get('error')})
            except Exception as ex:
                results.append({'id': acc_id, 'phone': account.phone, 'status': 'error', 'health': 'unknown', 'error': str(ex)})
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── BACKUP & RESTORE ─────────────────────────────────────────────────────────

@app.route('/api/export_sessions')
@login_required
def export_sessions():
    try:
        if is_admin():
            accounts = TelegramAccount.query.all()
        else:
            accounts = TelegramAccount.query.filter_by(user_id=current_user.id).all()
        data = [{
            'id': a.id,
            'phone': a.phone,
            'session_string': a.session_string,
            'user_id': a.user_id,
            'is_active': a.is_active,
            'added_at': a.added_at.isoformat() if a.added_at else None,
            'total_messages': a.total_messages,
            'total_reports': a.total_reports,
        } for a in accounts]
        return jsonify({'success': True, 'backup': data, 'count': len(data), 'exported_at': datetime.utcnow().isoformat()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/import_sessions', methods=['POST'])
@login_required
def import_sessions():
    try:
        data = request.get_json()
        backup = data.get('backup', [])
        if not isinstance(backup, list):
            return jsonify({'success': False, 'error': 'Invalid backup format'})
        imported = 0
        skipped = 0
        for item in backup:
            phone = item.get('phone', '').strip()
            session_string = item.get('session_string', '').strip()
            if not phone or not session_string:
                skipped += 1
                continue
            existing = TelegramAccount.query.filter_by(user_id=current_user.id, phone=phone).first()
            if existing:
                existing.session_string = session_string
                existing.is_active = True
                skipped += 1
            else:
                acc = TelegramAccount(user_id=current_user.id, phone=phone, session_string=session_string, is_active=True)
                db.session.add(acc)
                imported += 1
        db.session.commit()
        log_activity(current_user.id, f"Imported {imported} sessions, updated {skipped}", 'success')
        return jsonify({'success': True, 'message': f'Imported {imported} new accounts, updated {skipped} existing', 'imported': imported, 'updated': skipped})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── AUTO-REPLY RULES ─────────────────────────────────────────────────────────

@app.route('/api/auto_reply/rules')
@login_required
def get_auto_reply_rules():
    try:
        rules = AutoReplyRule.query.filter_by(user_id=current_user.id).order_by(AutoReplyRule.created_at.desc()).all()
        return jsonify({'success': True, 'rules': [{
            'id': r.id,
            'account_id': r.account_id,
            'keyword': r.keyword,
            'reply_text': r.reply_text,
            'match_type': r.match_type,
            'is_active': r.is_active,
            'trigger_count': r.trigger_count,
        } for r in rules]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/auto_reply/rules', methods=['POST'])
@login_required
def save_auto_reply_rule():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id', 0))
        keyword = (data.get('keyword') or '').strip()
        reply_text = (data.get('reply_text') or '').strip()
        match_type = data.get('match_type', 'contains')
        rule_id = data.get('id')
        if not keyword or not reply_text:
            return jsonify({'success': False, 'error': 'Keyword and reply text required'})
        account = TelegramAccount.query.get(account_id)
        if not account or not can_access_account(account):
            return jsonify({'success': False, 'error': 'Account not found or access denied'})
        if rule_id:
            rule = AutoReplyRule.query.filter_by(id=rule_id, user_id=current_user.id).first()
            if rule:
                rule.keyword = keyword
                rule.reply_text = reply_text
                rule.match_type = match_type
        else:
            rule = AutoReplyRule(user_id=current_user.id, account_id=account_id, keyword=keyword, reply_text=reply_text, match_type=match_type)
            db.session.add(rule)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Auto-reply rule saved'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/auto_reply/rules/<int:rule_id>', methods=['DELETE'])
@login_required
def delete_auto_reply_rule(rule_id):
    try:
        rule = AutoReplyRule.query.filter_by(id=rule_id, user_id=current_user.id).first()
        if not rule:
            return jsonify({'success': False, 'error': 'Rule not found'})
        db.session.delete(rule)
        db.session.commit()
        return jsonify({'success': True, 'message': 'Rule deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/auto_reply/toggle/<int:rule_id>', methods=['POST'])
@login_required
def toggle_auto_reply_rule(rule_id):
    try:
        rule = AutoReplyRule.query.filter_by(id=rule_id, user_id=current_user.id).first()
        if not rule:
            return jsonify({'success': False, 'error': 'Rule not found'})
        rule.is_active = not rule.is_active
        db.session.commit()
        return jsonify({'success': True, 'is_active': rule.is_active})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── BLACKLIST ────────────────────────────────────────────────────────────────

@app.route('/api/blacklist')
@login_required
def get_blacklist():
    try:
        items = MessageBlacklist.query.filter_by(user_id=current_user.id).order_by(MessageBlacklist.added_at.desc()).all()
        return jsonify({'success': True, 'blacklist': [{
            'id': i.id,
            'identifier': i.identifier,
            'reason': i.reason,
            'added_at': i.added_at.strftime('%Y-%m-%d %H:%M'),
        } for i in items]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/blacklist', methods=['POST'])
@login_required
def add_to_blacklist():
    try:
        data = request.get_json()
        identifier = (data.get('identifier') or '').strip()
        reason = (data.get('reason') or '').strip()
        if not identifier:
            return jsonify({'success': False, 'error': 'Identifier required'})
        existing = MessageBlacklist.query.filter_by(user_id=current_user.id, identifier=identifier).first()
        if existing:
            return jsonify({'success': False, 'error': 'Already in blacklist'})
        item = MessageBlacklist(user_id=current_user.id, identifier=identifier, reason=reason)
        db.session.add(item)
        db.session.commit()
        return jsonify({'success': True, 'message': f'Added {identifier} to blacklist'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/blacklist/<int:item_id>', methods=['DELETE'])
@login_required
def remove_from_blacklist(item_id):
    try:
        item = MessageBlacklist.query.filter_by(id=item_id, user_id=current_user.id).first()
        if not item:
            return jsonify({'success': False, 'error': 'Item not found'})
        db.session.delete(item)
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/blacklist/import', methods=['POST'])
@login_required
def import_blacklist():
    try:
        data = request.get_json()
        identifiers = data.get('identifiers', [])
        added = 0
        for ident in identifiers:
            ident = str(ident).strip()
            if not ident:
                continue
            if not MessageBlacklist.query.filter_by(user_id=current_user.id, identifier=ident).first():
                db.session.add(MessageBlacklist(user_id=current_user.id, identifier=ident))
                added += 1
        db.session.commit()
        return jsonify({'success': True, 'message': f'Added {added} items to blacklist'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── INTERVAL SCHEDULER ───────────────────────────────────────────────────────

@app.route('/api/schedule_interval', methods=['POST'])
@login_required
def schedule_interval():
    try:
        data = request.get_json()
        account_id = int(data.get('account_id', 0))
        content = (data.get('content') or '').strip()
        groups_raw = (data.get('groups') or '').strip()
        interval_minutes = int(data.get('interval_minutes', 60))
        delay = int(data.get('delay', 3))
        message_type = data.get('message_type', 'text')
        if not content or not groups_raw:
            return jsonify({'success': False, 'error': 'Content and groups required'})
        if interval_minutes < 1:
            return jsonify({'success': False, 'error': 'Interval must be at least 1 minute'})
        account = TelegramAccount.query.get(account_id)
        if not account or not can_access_account(account):
            return jsonify({'success': False, 'error': 'Account not found or access denied'})
        now = datetime.utcnow()
        sched = ScheduledBroadcast(
            user_id=current_user.id,
            account_id=account_id,
            message_type=message_type,
            content=content,
            groups=groups_raw,
            delay=delay,
            scheduled_at=now,
            status='pending',
            repeat_interval_minutes=interval_minutes,
            next_run_at=now,
        )
        db.session.add(sched)
        db.session.commit()
        log_activity(current_user.id, f"Created interval schedule: every {interval_minutes}min to {groups_raw[:50]}", 'success', account_id)
        return jsonify({'success': True, 'message': f'Interval scheduler created: every {interval_minutes} minutes', 'id': sched.id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/cancel_interval/<int:sched_id>', methods=['POST'])
@login_required
def cancel_interval(sched_id):
    try:
        sched = ScheduledBroadcast.query.filter_by(id=sched_id, user_id=current_user.id).first()
        if not sched:
            return jsonify({'success': False, 'error': 'Schedule not found'})
        sched.status = 'cancelled'
        db.session.commit()
        return jsonify({'success': True, 'message': 'Interval schedule cancelled'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/get_interval_schedules')
@login_required
def get_interval_schedules():
    try:
        scheds = ScheduledBroadcast.query.filter_by(user_id=current_user.id).filter(
            ScheduledBroadcast.repeat_interval_minutes != None
        ).order_by(ScheduledBroadcast.created_at.desc()).all()
        return jsonify({'success': True, 'schedules': [{
            'id': s.id,
            'account_id': s.account_id,
            'content': s.content[:100],
            'groups': s.groups[:100],
            'interval_minutes': s.repeat_interval_minutes,
            'status': s.status,
            'run_count': s.run_count,
            'next_run_at': s.next_run_at.strftime('%Y-%m-%d %H:%M') if s.next_run_at else '—',
        } for s in scheds]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── STARTUP ─────────────────────────────────────────────────────────────────

def migrate_db():
    """Add new columns to existing tables without dropping data."""
    migrations = [
        ("ALTER TABLE user ADD COLUMN is_banned BOOLEAN DEFAULT 0", "user.is_banned"),
        ("ALTER TABLE user ADD COLUMN ban_reason VARCHAR(300)", "user.ban_reason"),
        ("ALTER TABLE user ADD COLUMN last_login_at DATETIME", "user.last_login_at"),
        ("ALTER TABLE user ADD COLUMN last_login_ip VARCHAR(50)", "user.last_login_ip"),
        ("ALTER TABLE scheduled_broadcast ADD COLUMN repeat_interval_minutes INTEGER", "scheduled_broadcast.repeat_interval_minutes"),
        ("ALTER TABLE scheduled_broadcast ADD COLUMN next_run_at DATETIME", "scheduled_broadcast.next_run_at"),
        ("ALTER TABLE scheduled_broadcast ADD COLUMN run_count INTEGER DEFAULT 0", "scheduled_broadcast.run_count"),
        ("ALTER TABLE scheduled_broadcast ADD COLUMN created_at DATETIME", "scheduled_broadcast.created_at"),
    ]
    with db.engine.connect() as conn:
        for sql, name in migrations:
            try:
                conn.execute(db.text(sql))
                conn.commit()
                print(f"✅ Migration applied: {name}")
            except Exception:
                pass  # Column already exists


def init_db():
    db.create_all()
    migrate_db()

    admin = User.query.filter_by(username='admin').first()
    if not admin:
        admin = User(username='admin', password=generate_password_hash('Rasel412'))
        db.session.add(admin)
        db.session.flush()
        print("✅ Admin created: admin / Rasel412")
    else:
        admin.password = generate_password_hash('Rasel412')
        print("✅ Admin password updated: admin / Rasel412")

    user = User.query.filter_by(username='user').first()
    if not user:
        user = User(username='user', password=generate_password_hash('Rasel412'))
        db.session.add(user)
        print("✅ User created: user / Rasel412")
    else:
        user.password = generate_password_hash('Rasel412')
        print("✅ User password updated: user / Rasel412")

    if PaymentMethod.query.count() == 0:
        default_methods = [
            PaymentMethod(currency='USDT', network='TRC20', address='TG5oUoBj5RqkzRbLT5wJhNyR4RXEhMg9t7', display_order=0),
            PaymentMethod(currency='BTC',  network='Bitcoin', address='1CpLH2LXqYhfPvfPswV3XfQwcXwixypAUY', display_order=1),
            PaymentMethod(currency='TRX',  network='TRC20', address='TG5oUoBj5RqkzRbLT5wJhNyR4RXEhMg9t7', display_order=2),
            PaymentMethod(currency='ETH',  network='ERC20', address='0x82de5aa742b767a04e1c3fd1e0894a878a3dc71c', display_order=3),
        ]
        for m in default_methods:
            db.session.add(m)
        print("✅ Default payment methods added")

    db.session.commit()


def free_port(port):
    """Kill any process currently listening on the given port (excluding self)."""
    import signal as _sig
    own_pid = os.getpid()
    try:
        port_hex = format(port, '04X')
        with open('/proc/net/tcp') as f:
            lines = f.readlines()[1:]
        inodes = set()
        for line in lines:
            p = line.split()
            if len(p) > 10 and p[1].split(':')[-1].upper() == port_hex and p[3] == '0A':
                inodes.add(p[9])
        if not inodes:
            return
        for pid_str in os.listdir('/proc'):
            if not pid_str.isdigit():
                continue
            pid = int(pid_str)
            if pid == own_pid:
                continue
            try:
                for fd in os.listdir(f'/proc/{pid_str}/fd'):
                    try:
                        link = os.readlink(f'/proc/{pid_str}/fd/{fd}')
                        if any(f'socket:[{i}]' in link for i in inodes):
                            print(f"⚠️  Killing stale process PID {pid} on port {port}")
                            os.kill(pid, _sig.SIGKILL)
                            time.sleep(1)
                            break
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='TG Automation Dashboard')
    parser.add_argument('--port', type=int, default=None, help='Port to run on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    port = args.port or int(os.environ.get('PORT', 5000))

    # Clear any stale process on the port before binding
    free_port(port)

    with app.app_context():
        init_db()

    schedule_daily_reset()
    run_scheduled_broadcasts()
    print(f"\n🚀 TG Automation Dashboard running at http://{args.host}:{port}")
    print("🔑 Admin: admin / Rasel412")
    print("🔑 User:  user  / Rasel412\n")
    app.run(host=args.host, port=port, debug=args.debug, threaded=True)
