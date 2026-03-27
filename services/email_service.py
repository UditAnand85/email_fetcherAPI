import imaplib
import email
from email.utils import parsedate_to_datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
import re

def classify_emails_df(df, text_column='cleaned_body'):
    
    # ─── Keyword Lists ─────────────────────────────
    spam_keywords = [
        'winner', 'won', 'prize', 'lottery', 'claim', 'free', 'click here',
        'urgent', 'act now', 'limited time', 'congratulations', 'selected',
        'million dollars', 'inheritance', 'nigerian', 'wire transfer',
        'make money', 'earn money', 'work from home', 'extra income',
        'risk free', 'no credit check', 'guaranteed', 'cash prize',
        'you have been chosen', 'dear friend', 'dear beneficiary',
        'double your', 'increase your income', 'special promotion',
        'unsubscribe', 'opt out', 'this is not spam', 'not a scam',
        'cheap', 'buy now', 'order now', 'amazing offer', 'once in a lifetime',
        'weight loss', 'lose weight', 'diet pills', 'miracle cure',
        'enlarge', 'viagra', 'casino', 'betting', 'gambling', 'jackpot'
    ]

    work_keywords = [
        'meeting', 'project', 'deadline', 'report', 'update', 'attached',
        'please find', 'as discussed', 'follow up', 'follow-up', 'action required',
        'schedule', 'agenda', 'conference', 'presentation', 'proposal',
        'invoice', 'contract', 'agreement', 'review', 'approval', 'approved',
        'budget', 'quarterly', 'annual', 'strategy', 'team', 'colleague',
        'client', 'manager', 'department', 'office', 'task', 'assignment',
        'sprint', 'standup', 'scrum', 'jira', 'github', 'pull request',
        'deployment', 'release', 'server', 'database', 'pipeline',
        'hr', 'human resources', 'payroll', 'leave', 'attendance',
        'interview', 'hiring', 'job offer', 'onboarding', 'training',
        'performance', 'kpi', 'target', 'milestone', 'deliverable',
        'regards', 'sincerely', 'best regards', 'yours truly'
    ]

    promotion_keywords = [
        'sale', 'discount', 'offer', '% off', 'percent off', 'deal',
        'coupon', 'promo code', 'voucher', 'shop now', 'buy now',
        'exclusive', 'limited offer', 'flash sale', 'today only',
        'hurry', 'expires', 'clearance', 'new arrival', 'just launched',
        'subscribe', 'membership', 'upgrade', 'plan', 'pricing',
        'black friday', 'cyber monday', 'holiday sale', 'seasonal',
        'brand new', 'check out', 'browse', 'collection', 'store',
        'amazon', 'flipkart', 'myntra', 'zomato', 'swiggy', 'netflix',
        'spotify', 'youtube premium', 'special offer', 'reward points',
        'cashback', 'wallet', 'bonus', 'gift card', 'referral',
        'newsletter', 'weekly deals', 'top picks', 'recommended for you',
        'don t miss', 'save big', 'lowest price', 'best price'
    ]

    security_keywords = [
        'password', 'reset password', 'change password', 'verify',
        'verification', 'otp', 'one time password', 'two factor',
        '2fa', 'authentication', 'login attempt', 'sign in attempt',
        'suspicious', 'unauthorized', 'unusual activity', 'security alert',
        'account locked', 'account suspended', 'confirm your email',
        'confirm your account', 'click to verify', 'identity',
        'phishing', 'breach', 'compromised', 'protect your account',
        'secure your account', 'update your information', 'billing information',
        'payment failed', 'card declined', 'bank', 'transaction',
        'fraud', 'scam alert', 'warning', 'immediate action',
        'your account has been', 'access granted', 'access denied',
        'new device', 'new login', 'ip address', 'location detected'
    ]

    personal_keywords = [
        'hi', 'hello', 'hey', 'how are you', 'hope you are',
        'happy birthday', 'congratulations', 'good morning', 'good evening',
        'family', 'friend', 'miss you', 'see you', 'catch up',
        'dinner', 'lunch', 'party', 'wedding', 'invitation',
        'vacation', 'trip', 'holiday', 'weekend', 'hangout',
        'love', 'take care', 'stay safe', 'thinking of you',
        'how have you been', 'long time', 'get together'
    ]


    # ─── Helper: Count keywords ───────────────────
    def count_keywords(text, keywords):
        text = text.lower()
        return sum(1 for keyword in keywords if keyword in text)

    # ─── Core classifier ──────────────────────────
    def classify_email(text):
        if not isinstance(text, str) or text.strip() == '':
            return 'unknown'

        scores = {
            'spam':      count_keywords(text, spam_keywords),
            'work':      count_keywords(text, work_keywords),
            'promotion': count_keywords(text, promotion_keywords),
            'security':  count_keywords(text, security_keywords),
            'personal':  count_keywords(text, personal_keywords)
        }

        text_lower = text.lower()

        # Boost rules
        if re.search(r'\$[\d,]+', text): scores['spam'] += 2
        if text.count('!') > 3: scores['spam'] += 2
        if re.search(r'(free|win|won).{0,20}(click|now|today)', text_lower):
            scores['spam'] += 3

        if re.search(r'\b(please|kindly).{0,20}(find|review|check|confirm)\b', text_lower):
            scores['work'] += 3

        if re.search(r'\d+\s*%\s*off', text_lower):
            scores['promotion'] += 3

        if re.search(r'\b(click|verify|confirm).{0,20}(link|below|here|button)\b', text_lower):
            scores['security'] += 3

        if re.search(r'\b(let me know|looking forward)\b', text_lower):
            scores['personal'] += 2

        best_label = max(scores, key=scores.get)
        return best_label if scores[best_label] > 0 else 'unknown'

    # ─── Apply on DataFrame ───────────────────────
    df = df.copy()  # avoid modifying original
    df['label'] = df[text_column].apply(classify_email)

    return df


def add_cleaned_body(df, source_column='body', use_subject=True):
    
    def clean_email_body(text):
        if not isinstance(text, str):
            return ''

        # Remove HTML
        text = BeautifulSoup(text, 'html.parser').get_text()

        # Remove emails
        text = re.sub(r'\S+@\S+', '', text)

        # Remove URLs
        text = re.sub(r'http\S+|www\S+', '', text)

        # Remove special characters
        text = re.sub(r'[^a-zA-Z\s]', '', text)

        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text).strip()

        # Lowercase
        return text.lower()

    df = df.copy()

    # Combine subject + body (optional but recommended)
    if use_subject and 'subject' in df.columns:
        df['combined'] = df['subject'].fillna('') + ' ' + df[source_column].fillna('')
        df['cleaned_body'] = df['combined'].apply(clean_email_body)
    else:
        df['cleaned_body'] = df[source_column].apply(clean_email_body)

    return df
def fetch_emails_to_df(USER, PASSWORD, IMAP_URL='imap.gmail.com', max_emails=500):
    
    # ─── Helper: Optimal settings ───────────────────────────
    def get_optimal_settings(total_emails):
        if total_emails < 1000:
            return {'batch_size': 50,  'threads': 5}
        elif total_emails < 5000:
            return {'batch_size': 100, 'threads': 8}
        elif total_emails < 20000:
            return {'batch_size': 150, 'threads': 10}
        else:
            return {'batch_size': 200, 'threads': 10}

    # ─── Helper: Extract body ───────────────────────────────
    def extract_body(my_msg):
        body = ''
        if my_msg.is_multipart():
            for part in my_msg.walk():
                if part.get_content_type() == 'text/plain':
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        body = part.get_payload(decode=True).decode(charset, errors='replace')
                    except:
                        body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                    break
        else:
            charset = my_msg.get_content_charset() or 'utf-8'
            try:
                body = my_msg.get_payload(decode=True).decode(charset, errors='replace')
            except:
                body = my_msg.get_payload(decode=True).decode('utf-8', errors='replace')
        return body.strip()

    # ─── Helper: Parse date ─────────────────────────────────
    def parse_date(raw_date):
        try:
            return parsedate_to_datetime(raw_date) if raw_date else None
        except:
            return None

    # ─── Helper: Fetch batch ────────────────────────────────
    def fetch_batch(batch):
        try:
            conn = imaplib.IMAP4_SSL(IMAP_URL)
            conn.login(USER, PASSWORD)

            try:
                status, _ = conn.select('"[Gmail]/All Mail"', readonly=True)
                if status != 'OK':
                    conn.select('INBOX', readonly=True)
            except:
                conn.select('INBOX', readonly=True)

            batch_ids = b','.join(batch)
            _, data = conn.fetch(batch_ids, '(FLAGS BODY.PEEK[])')

            results = []
            for part in data:
                if isinstance(part, tuple):
                    try:
                        flags_raw = part[0].decode(errors='replace')
                        is_read = '\\Seen' in flags_raw

                        msg = email.message_from_bytes(part[1])
                        results.append({
                            'from': msg['from'] or '',
                            'subject': msg['subject'] or '',
                            'date': parse_date(msg.get('date')),
                            'status': 'Read' if is_read else 'Unread',
                            'body': extract_body(msg)
                        })
                    except:
                        continue

            conn.logout()
            return results

        except:
            return []

    # ─── Step 1: Get email IDs ─────────────────────────────
    conn = imaplib.IMAP4_SSL(IMAP_URL)
    conn.login(USER, PASSWORD)

    try:
        status, _ = conn.select('"[Gmail]/All Mail"', readonly=True)
        if status != 'OK':
            conn.select('INBOX', readonly=True)
    except:
        conn.select('INBOX', readonly=True)

    _, data = conn.search(None, 'ALL')
    mail_ids_list = data[0].split()

    # LIMIT emails (important for API speed)
    mail_ids_list = mail_ids_list[-max_emails:]
    conn.logout()

    total = len(mail_ids_list)
    if total == 0:
        return pd.DataFrame(columns=['from', 'subject', 'date', 'status', 'body'])

    # ─── Step 2: Auto tuning ───────────────────────────────
    settings = get_optimal_settings(total)
    BATCH_SIZE = settings['batch_size']
    THREADS = settings['threads']

    # ─── Step 3: Create batches ────────────────────────────
    batches = [mail_ids_list[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    # ─── Step 4: Parallel fetch ────────────────────────────
    emails_data = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(fetch_batch, batch) for batch in batches]

        for future in as_completed(futures):
            result = future.result()
            if result:
                emails_data.extend(result)

    # ─── Step 5: DataFrame ─────────────────────────────────
    df = pd.DataFrame(emails_data, columns=['from', 'subject', 'date', 'status', 'body'])

    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
    df.sort_values('date', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = add_cleaned_body(df)
    df = classify_emails_df(df)
    label_counts = df['label'].value_counts().to_dict()
    return label_counts



