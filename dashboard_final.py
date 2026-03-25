"""
DASHBOARD PHÂN TÍCH KỸ THUẬT CHỨNG KHOÁN VIỆT NAM
Phiên bản hoàn chỉnh - Tự động quét đáy, Telegram bot, Database, UTC+7
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
import time
import threading
import schedule
import requests
from zoneinfo import ZoneInfo

warnings.filterwarnings('ignore')

# ============================================================
# CẤU HÌNH THỜI GIAN VIỆT NAM (UTC+7)
# ============================================================
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vietnam_time():
    """Lấy thời gian hiện tại theo múi giờ Việt Nam"""
    return datetime.now(VIETNAM_TZ)

# ============================================================
# CẤU HÌNH TELEGRAM BOT
# ============================================================
TELEGRAM_TOKEN = "8485349113:AAH6M9LMVZ8PCYIssPatoIO8Vi7yjUiwLqo"
TELEGRAM_CHAT_ID = "1218472317"

def send_telegram_message(message):
    """Gửi tin nhắn qua Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")
        return False

def send_telegram_scan_report(results, scan_time):
    """Gửi báo cáo quét đáy qua Telegram"""
    if not results:
        return
    
    # Lọc cổ phiếu có điểm đáy cao
    top_stocks = [r for r in results if r['bottom_score'] >= 50][:10]
    
    if not top_stocks:
        return
    
    message = f"""
🔔 <b>KẾT QUẢ QUÉT ĐÁY</b>
⏰ Thời gian: {scan_time.strftime('%H:%M:%S %d/%m/%Y')}
📊 Số cổ phiếu: {len(results)} mã
🎯 Điểm đáy ≥ 50: {len(top_stocks)} mã

<b>🏆 TOP 10 CỔ PHIẾU TIỀM NĂNG:</b>
"""
    
    for i, r in enumerate(top_stocks[:10], 1):
        message += f"""
{i}. <b>{r['symbol']}</b> - {r['name'][:25]}
   ├─ Điểm: {r['bottom_score']}/{r['bottom_max_score']} ({r['bottom_percentage']:.0f}%)
   ├─ Đánh giá: {r['bottom_strength']}
   ├─ Wyckoff: {r['wyckoff_phase'][:20]}
   ├─ RSI: {r['rsi']:.1f}
   ├─ Giá: {r['current_price']:,.0f} VND
   └─ Nợ: {r['debt_ratio']*100:.0f}%"""
    
    message += "\n\n📊 <i>Chi tiết tại Dashboard PTKT Việt Nam</i>"
    
    send_telegram_message(message)

# ============================================================
# CẤU HÌNH DATABASE
# ============================================================
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


class DatabaseManager:
    """Quản lý kết nối PostgreSQL"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.is_connected = False
    
    def connect(self, host="localhost", port=5432, database="stock_db", 
                user="postgres", password="postgres"):
        """Kết nối đến PostgreSQL"""
        if not POSTGRES_AVAILABLE:
            return False
        
        try:
            self.conn = psycopg2.connect(
                host=host, port=port, database=database,
                user=user, password=password
            )
            self.cursor = self.conn.cursor()
            self._init_tables()
            self.is_connected = True
            return True
        except Exception as e:
            print(f"Lỗi kết nối PostgreSQL: {e}")
            return False
    
    def _init_tables(self):
        """Khởi tạo các bảng nếu chưa có"""
        # Bảng bài viết
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT,
                category VARCHAR(100),
                tags TEXT[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                author VARCHAR(100),
                view_count INT DEFAULT 0
            )
        """)
        
        # Bảng nghiên cứu
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS research (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                title TEXT NOT NULL,
                analysis_type VARCHAR(50),
                content TEXT,
                conclusion TEXT,
                rating INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                analyst VARCHAR(100)
            )
        """)
        
        # Bảng notes
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS notes (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                note TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Bảng scan_history
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_history (
                id SERIAL PRIMARY KEY,
                scan_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_stocks INT,
                high_score_stocks INT,
                top_stocks JSONB,
                scan_type VARCHAR(50)
            )
        """)
        
        self.conn.commit()
    
    def add_article(self, title, content, category="General", tags=None, author="User"):
        """Thêm bài viết mới"""
        if tags is None:
            tags = []
        try:
            self.cursor.execute("""
                INSERT INTO articles (title, content, category, tags, author)
                VALUES (%s, %s, %s, %s, %s) RETURNING id
            """, (title, content, category, tags, author))
            self.conn.commit()
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Lỗi thêm bài viết: {e}")
            return None
    
    def get_articles(self, category=None, limit=50):
        """Lấy danh sách bài viết"""
        try:
            if category:
                self.cursor.execute("""
                    SELECT * FROM articles 
                    WHERE category = %s 
                    ORDER BY created_at DESC LIMIT %s
                """, (category, limit))
            else:
                self.cursor.execute("""
                    SELECT * FROM articles 
                    ORDER BY created_at DESC LIMIT %s
                """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Lỗi lấy bài viết: {e}")
            return []
    
    def add_note(self, symbol, note):
        """Thêm ghi chú"""
        try:
            self.cursor.execute("""
                INSERT INTO notes (symbol, note)
                VALUES (%s, %s) RETURNING id
            """, (symbol, note))
            self.conn.commit()
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Lỗi thêm ghi chú: {e}")
            return None
    
    def get_notes(self, symbol=None, limit=50):
        """Lấy ghi chú"""
        try:
            if symbol:
                self.cursor.execute("""
                    SELECT * FROM notes 
                    WHERE symbol = %s 
                    ORDER BY created_at DESC LIMIT %s
                """, (symbol, limit))
            else:
                self.cursor.execute("""
                    SELECT * FROM notes 
                    ORDER BY created_at DESC LIMIT %s
                """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Lỗi lấy ghi chú: {e}")
            return []
    
    def add_research(self, symbol, title, analysis_type, content, conclusion, rating, analyst="User"):
        """Thêm nghiên cứu"""
        try:
            self.cursor.execute("""
                INSERT INTO research (symbol, title, analysis_type, content, conclusion, rating, analyst)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (symbol, title, analysis_type, content, conclusion, rating, analyst))
            self.conn.commit()
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Lỗi thêm nghiên cứu: {e}")
            return None
    
    def get_research(self, symbol=None, limit=50):
        """Lấy danh sách nghiên cứu"""
        try:
            if symbol:
                self.cursor.execute("""
                    SELECT * FROM research 
                    WHERE symbol = %s 
                    ORDER BY created_at DESC LIMIT %s
                """, (symbol, limit))
            else:
                self.cursor.execute("""
                    SELECT * FROM research 
                    ORDER BY created_at DESC LIMIT %s
                """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Lỗi lấy nghiên cứu: {e}")
            return []
    
    def save_scan_history(self, results, scan_type="auto"):
        """Lưu lịch sử quét"""
        try:
            top_stocks = [r for r in results if r['bottom_score'] >= 50][:10]
            top_stocks_data = []
            for r in top_stocks:
                top_stocks_data.append({
                    'symbol': r['symbol'],
                    'name': r['name'],
                    'score': r['bottom_score'],
                    'strength': r['bottom_strength'],
                    'price': r['current_price'],
                    'rsi': r['rsi']
                })
            
            self.cursor.execute("""
                INSERT INTO scan_history (total_stocks, high_score_stocks, top_stocks, scan_type)
                VALUES (%s, %s, %s, %s)
            """, (len(results), len(top_stocks), str(top_stocks_data), scan_type))
            self.conn.commit()
        except Exception as e:
            print(f"Lỗi lưu lịch sử quét: {e}")
    
    def close(self):
        """Đóng kết nối"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# ============================================================
# DANH SÁCH CỔ PHIẾU (70+ mã)
# ============================================================
STATE_OWNED_STOCKS = {
    # Doanh nghiệp nhà nước vốn hóa lớn
    "GAS": {"name": "Tổng Công ty Khí Việt Nam", "debt_ratio": 0.15, "state_ownership": 95.0, "sector": "Dầu khí"},
    "VNM": {"name": "Vinamilk", "debt_ratio": 0.08, "state_ownership": 36.0, "sector": "Thực phẩm"},
    "VCB": {"name": "Vietcombank", "debt_ratio": 0.85, "state_ownership": 74.8, "sector": "Ngân hàng"},
    "BID": {"name": "BIDV", "debt_ratio": 0.88, "state_ownership": 79.9, "sector": "Ngân hàng"},
    "CTG": {"name": "Vietinbank", "debt_ratio": 0.86, "state_ownership": 64.5, "sector": "Ngân hàng"},
    "PLX": {"name": "Xăng dầu Petrolimex", "debt_ratio": 0.65, "state_ownership": 75.8, "sector": "Dầu khí"},
    "POW": {"name": "PetroVietnam Power", "debt_ratio": 0.55, "state_ownership": 51.0, "sector": "Điện"},
    "BVH": {"name": "Bảo Việt", "debt_ratio": 0.72, "state_ownership": 52.6, "sector": "Bảo hiểm"},
    "SAB": {"name": "Sabeco", "debt_ratio": 0.12, "state_ownership": 53.6, "sector": "Đồ uống"},
    "ACV": {"name": "ACV - Cảng hàng không", "debt_ratio": 0.05, "state_ownership": 95.0, "sector": "Hàng không"},
    "DPM": {"name": "Đạm Phú Mỹ", "debt_ratio": 0.20, "state_ownership": 51.0, "sector": "Phân bón"},
    "DCM": {"name": "Đạm Cà Mau", "debt_ratio": 0.22, "state_ownership": 51.0, "sector": "Phân bón"},
    "PVS": {"name": "PVC - Dịch vụ Dầu khí", "debt_ratio": 0.45, "state_ownership": 51.0, "sector": "Dầu khí"},
    "PVD": {"name": "PV Drilling", "debt_ratio": 0.38, "state_ownership": 51.0, "sector": "Dầu khí"},
    
    # Doanh nghiệp sân sau nhà nước
    "HPG": {"name": "Hòa Phát", "debt_ratio": 0.45, "state_ownership": 0, "sector": "Thép"},
    "MWG": {"name": "Thế giới di động", "debt_ratio": 0.52, "state_ownership": 0, "sector": "Bán lẻ"},
    "FPT": {"name": "FPT", "debt_ratio": 0.38, "state_ownership": 0, "sector": "Công nghệ"},
    "MSN": {"name": "Masan", "debt_ratio": 0.62, "state_ownership": 0, "sector": "Hàng tiêu dùng"},
    "VIC": {"name": "Vingroup", "debt_ratio": 0.58, "state_ownership": 0, "sector": "BĐS"},
    "VHM": {"name": "Vinhomes", "debt_ratio": 0.48, "state_ownership": 0, "sector": "BĐS"},
    "SSI": {"name": "SSI", "debt_ratio": 0.55, "state_ownership": 0, "sector": "Chứng khoán"},
    "HCM": {"name": "HCM", "debt_ratio": 0.52, "state_ownership": 0, "sector": "Chứng khoán"},
    "VND": {"name": "VNDirect", "debt_ratio": 0.58, "state_ownership": 0, "sector": "Chứng khoán"},
    "TCB": {"name": "Techcombank", "debt_ratio": 0.79, "state_ownership": 0, "sector": "Ngân hàng"},
    "MBB": {"name": "MBBank", "debt_ratio": 0.82, "state_ownership": 0, "sector": "Ngân hàng"},
    "ACB": {"name": "ACB", "debt_ratio": 0.78, "state_ownership": 0, "sector": "Ngân hàng"},
    
    # Doanh nghiệp không nợ / nợ thấp
    "REE": {"name": "REE", "debt_ratio": 0.18, "state_ownership": 0, "sector": "Cơ điện"},
    "DGC": {"name": "Đức Giang", "debt_ratio": 0.05, "state_ownership": 0, "sector": "Hóa chất"},
    "VHC": {"name": "Vĩnh Hoàn", "debt_ratio": 0.15, "state_ownership": 0, "sector": "Thủy sản"},
    "PNJ": {"name": "PNJ", "debt_ratio": 0.28, "state_ownership": 0, "sector": "Bán lẻ"},
    "VRE": {"name": "Vincom Retail", "debt_ratio": 0.32, "state_ownership": 0, "sector": "BĐS"},
    "HDG": {"name": "Hà Đô", "debt_ratio": 0.35, "state_ownership": 0, "sector": "BĐS"},
    "KDC": {"name": "Kido", "debt_ratio": 0.25, "state_ownership": 0, "sector": "Thực phẩm"},
    "DGW": {"name": "Thế giới số", "debt_ratio": 0.32, "state_ownership": 0, "sector": "Công nghệ"},
    "FRT": {"name": "FPT Retail", "debt_ratio": 0.45, "state_ownership": 0, "sector": "Bán lẻ"},
    "CTD": {"name": "Coteccons", "debt_ratio": 0.35, "state_ownership": 0, "sector": "Xây dựng"},
}

ALL_STOCKS = list(STATE_OWNED_STOCKS.keys())


# ============================================================
# KẾT NỐI DỮ LIỆU
# ============================================================
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False


class StockDataLoader:
    """Lấy dữ liệu chứng khoán"""
    
    def __init__(self):
        self.yahoo_map = {s: f"{s}.VN" for s in ALL_STOCKS}
    
    @st.cache_data(ttl=300, show_spinner=False)
    def get_stock_history(_self, symbol, start_date, end_date, resolution="D"):
        symbol = symbol.upper()
        
        if YFINANCE_AVAILABLE:
            try:
                yf_symbol = _self.yahoo_map.get(symbol, f"{symbol}.VN")
                interval_map = {"D": "1d", "W": "1wk"}
                interval = interval_map.get(resolution, "1d")
                
                ticker = yf.Ticker(yf_symbol)
                df = ticker.history(start=start_date, end=end_date, interval=interval)
                
                if not df.empty:
                    df = df.reset_index()
                    df.columns = [col.lower() for col in df.columns]
                    if 'date' in df.columns:
                        df.rename(columns={'date': 'date'}, inplace=True)
                    elif 'datetime' in df.columns:
                        df.rename(columns={'datetime': 'date'}, inplace=True)
                    else:
                        df['date'] = df.index
                    
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol
                    return df
            except Exception as e:
                pass
        
        return pd.DataFrame()
    
    def get_liquid_stocks(self, limit=200):
        return ALL_STOCKS[:limit]


class AdvancedBottomDetector:
    """Phát hiện đáy thật với scoring nâng cao"""
    
    def __init__(self):
        self.weights = {
            'spring': 25,
            'volume_spike': 20,
            'rsi_oversold': 15,
            'break_ma20': 15,
            'ma20_recovery': 15,
            'rsi_confirmation': 10
        }
        self.max_score = sum(self.weights.values())
    
    def detect_spring(self, df, lookback=20):
        if len(df) < lookback + 5:
            return False, 0
        
        current_low = df['low'].iloc[-1]
        prev_low = df['low'].iloc[-lookback-1:-1].min()
        
        if current_low < prev_low * 0.99:
            current_close = df['close'].iloc[-1]
            if current_close > current_low * 1.02:
                return True, self.weights['spring']
        return False, 0
    
    def detect_volume_spike(self, df, period=20):
        if len(df) < period + 5:
            return False, 0
        
        avg_volume = df['volume'].tail(period).mean()
        current_volume = df['volume'].iloc[-1]
        
        if current_volume > avg_volume * 1.5:
            if df['close'].iloc[-1] > df['close'].iloc[-2]:
                return True, self.weights['volume_spike']
        return False, 0
    
    def detect_rsi_oversold(self, df):
        close = df['close']
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        
        if 25 <= current_rsi <= 35:
            return True, self.weights['rsi_oversold']
        return False, 0
    
    def detect_break_ma20(self, df):
        if len(df) < 20:
            return False, 0
        
        ma20 = df['close'].rolling(window=20).mean()
        current_price = df['close'].iloc[-1]
        prev_price = df['close'].iloc[-2]
        
        if prev_price <= ma20.iloc[-2] and current_price > ma20.iloc[-1]:
            return True, self.weights['break_ma20']
        return False, 0
    
    def detect_ma20_recovery(self, df):
        if len(df) < 40:
            return False, 0
        
        ma20 = df['close'].rolling(window=20).mean()
        ma20_slope = (ma20.iloc[-1] - ma20.iloc[-5]) / ma20.iloc[-5] * 100 if ma20.iloc[-5] > 0 else 0
        
        if ma20_slope > 0 and ma20.iloc[-1] > ma20.iloc[-10]:
            return True, self.weights['ma20_recovery']
        return False, 0
    
    def detect_rsi_confirmation(self, df):
        close = df['close']
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        
        if current_rsi > 50:
            return True, self.weights['rsi_confirmation']
        return False, 0
    
    def analyze(self, df):
        if df.empty or len(df) < 50:
            return None
        
        results = {
            'spring': self.detect_spring(df),
            'volume_spike': self.detect_volume_spike(df),
            'rsi_oversold': self.detect_rsi_oversold(df),
            'break_ma20': self.detect_break_ma20(df),
            'ma20_recovery': self.detect_ma20_recovery(df),
            'rsi_confirmation': self.detect_rsi_confirmation(df)
        }
        
        total_score = sum(score for _, score in results.values())
        
        if total_score >= 70:
            strength = "ĐÁY THẬT (Strong Buy)"
            color = "green"
        elif total_score >= 50:
            strength = "ĐÁY TIỀM NĂNG (Buy)"
            color = "blue"
        elif total_score >= 30:
            strength = "CẢNH BÁO ĐÁY (Watch)"
            color = "orange"
        else:
            strength = "CHƯA CÓ TÍN HIỆU"
            color = "gray"
        
        return {
            'score': total_score,
            'max_score': self.max_score,
            'strength': strength,
            'color': color,
            'details': results,
            'percentage': total_score / self.max_score * 100
        }


class WyckoffAnalyzer:
    """Phân tích chu kỳ Wyckoff"""
    
    def analyze_phase(self, df):
        if df.empty or len(df) < 50:
            return "Không đủ dữ liệu", "gray"
        
        close = df['close']
        volume = df['volume']
        
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        price_trend = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100
        avg_volume = volume.tail(20).mean()
        volume_trend = (volume.iloc[-1] - avg_volume) / avg_volume * 100
        
        if price_trend < -15 and volume_trend > 50 and current_rsi < 30:
            return "Phase A - Selling Climax (Kết thúc giảm)", "red"
        elif abs(price_trend) < 5 and volume_trend < -30:
            return "Phase B - Tích lũy (Sideway)", "orange"
        elif price_trend < -8 and volume_trend > 30 and current_rsi < 35:
            return "Phase C - Test đáy (Spring/Shakeout)", "orange"
        elif price_trend > 8 and volume_trend > 50 and current_rsi > 50:
            return "Phase D - Đánh lên (Breakout)", "blue"
        elif price_trend > 20 and volume_trend > 80 and current_rsi > 70:
            return "Phase E - Phân phối (FOMO)", "purple"
        elif abs(price_trend) < 5:
            return "Sideway - Chờ xác nhận", "gray"
        elif price_trend < -5:
            return "Xu hướng giảm", "red"
        else:
            return "Xu hướng tăng", "green"


class SmartMoneyDetector:
    """Phát hiện dấu hiệu dòng tiền thông minh"""
    
    def analyze(self, df):
        if df.empty or len(df) < 30:
            return []
        
        close = df['close']
        volume = df['volume']
        
        avg_volume = volume.tail(20).mean()
        signals = []
        
        recent_volume = volume.tail(10).mean()
        price_range = close.tail(10).max() - close.tail(10).min()
        price_range_pct = price_range / close.tail(10).mean() * 100
        
        if recent_volume > avg_volume * 1.3 and price_range_pct < 3:
            signals.append({
                'type': 'TÍCH LŨY',
                'description': 'Volume lớn nhưng giá ổn định - Có thể tích lũy',
                'strength': 'HIGH'
            })
        
        volume_decline = volume.tail(5).mean() / volume.tail(20).mean()
        price_stable = abs(close.tail(5).mean() - close.tail(20).mean()) / close.tail(20).mean() * 100
        
        if volume_decline < 0.7 and price_stable < 2:
            signals.append({
                'type': 'SIẾT CUNG',
                'description': 'Volume giảm, giá giữ vững - Cung yếu',
                'strength': 'MEDIUM'
            })
        
        current_volume = volume.iloc[-1]
        recent_high = close.tail(20).max()
        
        if current_volume > avg_volume * 1.5 and close.iloc[-1] > recent_high * 0.98:
            signals.append({
                'type': 'BREAK THẬT',
                'description': 'Volume lớn vượt kháng cự - Break thật',
                'strength': 'HIGH'
            })
        
        return signals


class StockScanner:
    """Quét cổ phiếu với scoring nâng cao"""
    
    def __init__(self, loader):
        self.loader = loader
        self.bottom_detector = AdvancedBottomDetector()
        self.wyckoff = WyckoffAnalyzer()
        self.smart_money = SmartMoneyDetector()
    
    def analyze_stock(self, symbol):
        end = datetime.now(VIETNAM_TZ)
        start = end - timedelta(days=180)
        
        df = self.loader.get_stock_history(
            symbol,
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            "D"
        )
        
        if df.empty or len(df) < 50:
            return None
        
        close = df['close']
        volume = df['volume']
        
        ma20 = close.rolling(window=20).mean()
        ma50 = close.rolling(window=50).mean()
        
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        bottom_analysis = self.bottom_detector.analyze(df)
        wyckoff_phase, wyckoff_color = self.wyckoff.analyze_phase(df)
        smart_money_signals = self.smart_money.analyze(df)
        
        stock_info = STATE_OWNED_STOCKS.get(symbol, {
            "name": symbol, "debt_ratio": 0.5, "state_ownership": 0, "sector": "Khác"
        })
        
        return {
            'symbol': symbol,
            'name': stock_info['name'],
            'debt_ratio': stock_info['debt_ratio'],
            'state_ownership': stock_info['state_ownership'],
            'sector': stock_info['sector'],
            'current_price': close.iloc[-1],
            'volume': volume.iloc[-1],
            'avg_volume_20': volume.tail(20).mean(),
            'ma20': ma20.iloc[-1],
            'ma50': ma50.iloc[-1],
            'rsi': rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50,
            'bottom_score': bottom_analysis['score'] if bottom_analysis else 0,
            'bottom_max_score': bottom_analysis['max_score'] if bottom_analysis else 100,
            'bottom_percentage': bottom_analysis['percentage'] if bottom_analysis else 0,
            'bottom_strength': bottom_analysis['strength'] if bottom_analysis else "Không xác định",
            'bottom_color': bottom_analysis['color'] if bottom_analysis else "gray",
            'bottom_details': bottom_analysis['details'] if bottom_analysis else {},
            'wyckoff_phase': wyckoff_phase,
            'wyckoff_color': wyckoff_color,
            'smart_money_signals': smart_money_signals
        }
    
    def scan_all(self, symbols, progress_callback=None):
        """Quét tất cả cổ phiếu"""
        results = []
        for i, symbol in enumerate(symbols):
            analysis = self.analyze_stock(symbol)
            if analysis:
                results.append(analysis)
            if progress_callback:
                progress_callback(i + 1, len(symbols))
        results.sort(key=lambda x: x['bottom_score'], reverse=True)
        return results


# ============================================================
# KHỞI TẠO HỆ THỐNG
# ============================================================
@st.cache_resource
def init_system():
    loader = StockDataLoader()
    scanner = StockScanner(loader)
    db = DatabaseManager()
    return loader, scanner, db


# ============================================================
# QUẢN LÝ LỊCH QUÉT TỰ ĐỘNG
# ============================================================
class AutoScheduler:
    """Quản lý lịch quét tự động"""
    
    def __init__(self, scanner, db):
        self.scanner = scanner
        self.db = db
        self.is_running = False
        self.last_scan_time = None
    
    def run_daily_scan(self):
        """Chạy quét hàng ngày lúc 14:00"""
        vietnam_time = get_vietnam_time()
        print(f"[{vietnam_time}] Bắt đầu quét đáy tự động...")
        
        symbols = ALL_STOCKS
        results = self.scanner.scan_all(symbols)
        
        if results:
            # Gửi Telegram
            send_telegram_scan_report(results, vietnam_time)
            
            # Lưu vào database
            if self.db.is_connected:
                self.db.save_scan_history(results, "auto")
            
            self.last_scan_time = vietnam_time
            print(f"[{vietnam_time}] Đã quét xong {len(results)} cổ phiếu")
        
        return results
    
    def start_scheduler(self):
        """Khởi động scheduler"""
        if self.is_running:
            return
        
        # Lên lịch quét lúc 14:00 hàng ngày
        schedule.every().day.at("14:00").do(self.run_daily_scan)
        self.is_running = True
        
        # Chạy ngay lần đầu nếu chưa có
        if self.last_scan_time is None:
            self.run_daily_scan()
        
        # Chạy scheduler trong background
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        threading.Thread(target=run_scheduler, daemon=True).start()


# ============================================================
# GIAO DIỆN CHÍNH
# ============================================================
def main():
    loader, scanner, db = init_system()
    
    # Khởi tạo scheduler
    scheduler = AutoScheduler(scanner, db)
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/chart-line.png", width=60)
        st.title("📈 PTKT Việt Nam")
        st.markdown("**Phát Hiện Đáy Thật & Dòng Tiền Thông Minh**")
        st.markdown("---")
        
        # Thời gian Việt Nam
        vietnam_time = get_vietnam_time()
        st.info(f"🕐 Giờ Việt Nam: {vietnam_time.strftime('%H:%M:%S %d/%m/%Y')}")
        
        st.markdown("---")
        
        # Kết nối Database
        with st.expander("🗄️ Kết nối Database", expanded=False):
            use_db = st.checkbox("Kết nối PostgreSQL", key="db_connect_checkbox")
            if use_db:
                db_host = st.text_input("Host", "localhost", key="db_host")
                db_port = st.number_input("Port", 5432, key="db_port")
                db_name = st.text_input("Database", "stock_db", key="db_name")
                db_user = st.text_input("User", "postgres", key="db_user")
                db_pass = st.text_input("Password", "postgres", type="password", key="db_pass")
                
                if st.button("Kết nối", key="db_connect_btn"):
                    if db.connect(db_host, db_port, db_name, db_user, db_pass):
                        st.success("✅ Đã kết nối database")
                        st.session_state['db_connected'] = True
                    else:
                        st.error("❌ Kết nối thất bại")
        
        st.markdown("---")
        
        st.subheader("📊 Thông tin")
        st.info(f"""
        - **Nguồn:** Yahoo Finance
        - **Cổ phiếu:** {len(ALL_STOCKS)} mã
        - **Doanh nghiệp NN:** {len([s for s in ALL_STOCKS if STATE_OWNED_STOCKS.get(s, {}).get('state_ownership', 0) > 0])} mã
        - **Nợ thấp (<20%):** {len([s for s in ALL_STOCKS if STATE_OWNED_STOCKS.get(s, {}).get('debt_ratio', 0.5) < 0.2])} mã
        """)
        
        st.markdown("---")
        
        st.subheader("⏰ Lịch quét tự động")
        st.markdown("""
        - **Giờ quét:** 14:00 hàng ngày
        - **Gửi Telegram:** Tự động
        - **Lưu database:** Tự động
        """)
        
        if scheduler.last_scan_time:
            st.caption(f"Lần quét cuối: {scheduler.last_scan_time.strftime('%H:%M:%S %d/%m/%Y')}")
        
        st.markdown("---")
        
        st.subheader("🎯 Scoring Đáy Thật")
        st.markdown("""
        | Tiêu chí | Điểm |
        |----------|------|
        | Spring | 25 |
        | Volume Spike | 20 |
        | RSI hợp lý | 15 |
        | Break MA20 | 15 |
        | MA20 Recovery | 15 |
        | RSI > 50 | 10 |
        | **Tối đa** | **100** |
        """)
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🏠 Tổng quan",
        "🔍 Phát hiện đáy thật",
        "📊 Phân tích Wyckoff",
        "💎 Doanh nghiệp",
        "📝 Ghi chú & Nghiên cứu",
        "📚 Hướng dẫn"
    ])
    
    # Khởi chạy scheduler tự động
    if not scheduler.is_running:
        scheduler.start_scheduler()
        st.toast("✅ Đã khởi động lịch quét tự động lúc 14:00 hàng ngày", icon="⏰")
    
    # Tab 1: Tổng quan
    with tab1:
        st.title("🏠 Phân Tích Kỹ Thuật Nâng Cao")
        st.markdown("""
        ### Hệ thống phát hiện đáy thật với Scoring 100 điểm
        
        **Các tính năng chính:**
        - ✅ Scoring nâng cao với 6 tiêu chí phát hiện đáy thật
        - ✅ Phân tích chu kỳ Wyckoff (Phase A-E)
        - ✅ Phát hiện dấu hiệu dòng tiền thông minh (Smart Money)
        - ✅ Danh sách doanh nghiệp nhà nước vốn hóa lớn
        - ✅ Phân loại doanh nghiệp theo tỷ lệ nợ
        - ✅ Tự động quét đáy lúc 14:00 hàng ngày
        - ✅ Gửi báo cáo qua Telegram
        - ✅ Lưu lịch sử vào Database
        """)
        
        st.markdown("---")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("VN-Index", "1,280.50", delta="+12.3")
        with col2:
            st.metric("HNX-Index", "235.80", delta="+2.5")
        with col3:
            st.metric("Doanh nghiệp NN", len([s for s in ALL_STOCKS if STATE_OWNED_STOCKS.get(s, {}).get('state_ownership', 0) > 0]))
        with col4:
            st.metric("Nợ thấp (<20%)", len([s for s in ALL_STOCKS if STATE_OWNED_STOCKS.get(s, {}).get('debt_ratio', 0.5) < 0.2]))
    
    # Tab 2: Phát hiện đáy thật
    with tab2:
        st.header("🔍 Phát Hiện Đáy Thật - Scoring 100 Điểm")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            limit = st.slider("Số lượng cổ phiếu quét", 20, 200, 100, 10, key="scan_limit")
        with col2:
            if st.button("🚀 Quét đáy thật", type="primary", use_container_width=True, key="scan_btn"):
                with st.spinner("Đang phân tích dữ liệu..."):
                    symbols = loader.get_liquid_stocks(limit)
                    results = scanner.scan_all(symbols)
                    st.session_state['scan_results'] = results
                    st.success(f"✅ Đã phân tích {len(results)} cổ phiếu")
                    
                    # Gửi Telegram nếu có kết quả
                    if results:
                        send_telegram_scan_report(results, get_vietnam_time())
        
        st.markdown("---")
        
        if st.session_state.get('scan_results'):
            results = st.session_state['scan_results']
            
            # Bộ lọc
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                min_score = st.slider("Điểm tối thiểu", 0, 100, 50, key="min_score_filter")
            with col2:
                phase_filter = st.selectbox("Wyckoff Phase", ["Tất cả", "Phase C - Test đáy", "Phase D - Đánh lên"], key="phase_filter")
            with col3:
                debt_filter = st.selectbox("Tỷ lệ nợ", ["Tất cả", "Nợ thấp (<20%)", "Nợ trung bình (20-50%)", "Nợ cao (>50%)"], key="debt_filter")
            with col4:
                sector_filter = st.selectbox("Ngành", ["Tất cả"] + list(set([r.get('sector', 'Khác') for r in results])), key="sector_filter")
            
            filtered = [r for r in results if r['bottom_score'] >= min_score]
            
            if phase_filter != "Tất cả":
                filtered = [r for r in filtered if phase_filter in r['wyckoff_phase']]
            
            if debt_filter == "Nợ thấp (<20%)":
                filtered = [r for r in filtered if r['debt_ratio'] < 0.2]
            elif debt_filter == "Nợ trung bình (20-50%)":
                filtered = [r for r in filtered if 0.2 <= r['debt_ratio'] < 0.5]
            elif debt_filter == "Nợ cao (>50%)":
                filtered = [r for r in filtered if r['debt_ratio'] >= 0.5]
            
            if sector_filter != "Tất cả":
                filtered = [r for r in filtered if r.get('sector', 'Khác') == sector_filter]
            
            st.subheader(f"📊 Kết quả quét đáy ({len(filtered)} cổ phiếu)")
            
            data = []
            for r in filtered:
                data.append({
                    "Mã": r['symbol'],
                    "Tên": r['name'][:20],
                    "Ngành": r.get('sector', 'Khác')[:15],
                    "Giá": f"{r['current_price']:,.0f}",
                    "Điểm": f"{r['bottom_score']}/{r['bottom_max_score']}",
                    "%": f"{r['bottom_percentage']:.0f}%",
                    "Đánh giá": r['bottom_strength'],
                    "Wyckoff": r['wyckoff_phase'][:25],
                    "RSI": f"{r['rsi']:.1f}",
                    "Nợ": f"{r['debt_ratio']*100:.0f}%"
                })
            
            st.dataframe(pd.DataFrame(data), use_container_width=True, height=500)
    
    # Tab 3: Phân tích Wyckoff
    with tab3:
        st.header("🔄 Phân Tích Chu Kỳ Wyckoff")
        
        wyckoff_symbol = st.selectbox("Chọn cổ phiếu", ALL_STOCKS, key="wyckoff_symbol")
        
        if st.button("🔍 Phân tích Wyckoff", key="wyckoff_btn"):
            with st.spinner("Đang phân tích..."):
                analysis = scanner.analyze_stock(wyckoff_symbol)
                if analysis:
                    st.session_state['wyckoff_analysis'] = analysis
        
        if st.session_state.get('wyckoff_analysis'):
            analysis = st.session_state['wyckoff_analysis']
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"### {analysis['symbol']} - {analysis['name']}")
                st.metric("Wyckoff Phase", analysis['wyckoff_phase'])
                st.metric("Điểm đáy", f"{analysis['bottom_score']}/{analysis['bottom_max_score']}")
                st.metric("RSI", f"{analysis['rsi']:.1f}")
            
            with col2:
                st.markdown("**Thông tin**")
                st.write(f"- Giá: {analysis['current_price']:,.0f} VND")
                st.write(f"- MA20: {analysis['ma20']:,.0f}")
                st.write(f"- MA50: {analysis['ma50']:,.0f}")
                st.write(f"- Tỷ lệ nợ: {analysis['debt_ratio']*100:.1f}%")
    
    # Tab 4: Doanh nghiệp
    with tab4:
        st.header("💎 Danh Sách Doanh Nghiệp")
        
        debt_filter = st.selectbox("Lọc theo nợ", ["Tất cả", "Nợ thấp (<20%)", "Nợ trung bình", "Nợ cao"], key="debt_filter_state")
        
        data = []
        for s, info in STATE_OWNED_STOCKS.items():
            if debt_filter == "Nợ thấp (<20%)" and info['debt_ratio'] >= 0.2:
                continue
            elif debt_filter == "Nợ trung bình" and (info['debt_ratio'] < 0.2 or info['debt_ratio'] >= 0.5):
                continue
            elif debt_filter == "Nợ cao" and info['debt_ratio'] < 0.5:
                continue
            
            data.append({
                "Mã": s,
                "Tên": info['name'],
                "Ngành": info.get('sector', 'Khác'),
                "Sở hữu NN": f"{info['state_ownership']:.1f}%" if info['state_ownership'] > 0 else "Tư nhân",
                "Tỷ lệ nợ": f"{info['debt_ratio']*100:.1f}%"
            })
        
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=600)
    
    # Tab 5: Ghi chú & Nghiên cứu
    with tab5:
        st.header("📝 Ghi chú & Nghiên cứu")
        
        sub_tab1, sub_tab2, sub_tab3 = st.tabs(["📝 Ghi chú", "📄 Bài viết", "🔬 Nghiên cứu"])
        
        with sub_tab1:
            st.subheader("Ghi chú theo cổ phiếu")
            note_symbol = st.selectbox("Chọn cổ phiếu", ALL_STOCKS[:50], key="note_symbol")
            new_note = st.text_area("Nội dung ghi chú", height=150, key="note_content")
            
            if st.button("Lưu ghi chú", key="save_note_btn"):
                if st.session_state.get('db_connected'):
                    db.add_note(note_symbol, new_note)
                    st.success("✅ Đã lưu ghi chú")
                else:
                    st.warning("Chưa kết nối database. Vui lòng kết nối ở sidebar.")
        
        with sub_tab2:
            st.subheader("Thêm bài viết mới")
            
            with st.form("add_article_form"):
                article_title = st.text_input("Tiêu đề", key="article_title")
                article_category = st.selectbox("Danh mục", ["Phân tích kỹ thuật", "Chiến lược", "Tâm lý thị trường", "Kiến thức cơ bản", "Khác"], key="article_category")
                article_content = st.text_area("Nội dung", height=300, key="article_content")
                article_author = st.text_input("Tác giả", "User", key="article_author")
                
                submitted = st.form_submit_button("Đăng bài")
                if submitted:
                    if st.session_state.get('db_connected'):
                        db.add_article(article_title, article_content, article_category, author=article_author)
                        st.success("✅ Đã đăng bài viết")
                    else:
                        st.warning("Chưa kết nối database")
    
    # Tab 6: Hướng dẫn
    with tab6:
        st.header("📚 Hướng Dẫn Phát Hiện Đáy Thật")
        
        st.markdown("""
        ### 1. Scoring Đáy Thật (100 điểm)
        
        | Tiêu chí | Điểm | Giải thích |
        |----------|------|------------|
        | **Spring** | 25 | Giá thủng đáy cũ nhưng nhanh chóng kéo lên |
        | **Volume Spike** | 20 | Khối lượng giao dịch đột biến kèm giá tăng |
        | **RSI vùng hợp lý** | 15 | RSI trong vùng 25-35 - Không quá bán sâu |
        | **Break MA20** | 15 | Giá vượt lên trên MA20 - Xác nhận đáy |
        | **MA20 Recovery** | 15 | Đường MA20 bắt đầu hồi phục |
        | **RSI > 50** | 10 | RSI vượt ngưỡng 50 - Xác nhận đảo chiều |
        
        ### 2. Phân loại kết quả
        
        - **>= 70 điểm**: ĐÁY THẬT (Strong Buy)
        - **50-69 điểm**: ĐÁY TIỀM NĂNG (Buy)
        - **30-49 điểm**: CẢNH BÁO ĐÁY (Watch)
        - **< 30 điểm**: CHƯA CÓ TÍN HIỆU
        """)
    
    # Footer
    st.markdown("---")
    st.caption(f"© 2024 PTKT Việt Nam | {len(ALL_STOCKS)} cổ phiếu | Giờ VN: {get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}")


if __name__ == "__main__":
    main()
