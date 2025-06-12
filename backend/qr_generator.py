# backend/qr_generator.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import qrcode
from PIL import Image, ImageDraw, ImageFont
import urllib.request
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# 개선된 한국어 폰트 URL
KOREAN_FONT_URLS = [
    # WOFF2/WOFF는 PIL이 지원하지 않으므로 TTF/OTF 사용
    "https://cdn.jsdelivr.net/gh/google/fonts/ofl/notosanskr/NotoSansKR-Regular.ttf",
    "https://fonts.gstatic.com/ea/notosanskr/v2/NotoSansKR-Regular.otf",
    "https://github.com/notofonts/noto-cjk/raw/main/Sans/OTF/Korean/NotoSansCJKkr-Regular.otf",
    "https://raw.githubusercontent.com/google/fonts/main/ofl/notosanskr/NotoSansKR-Regular.ttf"
]

def download_korean_font():
    """한국어 폰트 다운로드"""
    font_dir = Path("fonts")
    font_dir.mkdir(exist_ok=True)
    
    font_path = font_dir / "NotoSansKR-Regular.ttf"
    
    if font_path.exists() and font_path.stat().st_size > 100000:  # 100KB 이상
        return str(font_path)
    
    for i, font_url in enumerate(KOREAN_FONT_URLS):
        try:
            logger.info(f"📥 한국어 폰트 다운로드 시도 {i+1}/{len(KOREAN_FONT_URLS)}: {font_url}")
            urllib.request.urlretrieve(font_url, font_path)
            
            if font_path.exists() and font_path.stat().st_size > 100000:
                logger.info(f"✅ 폰트 다운로드 완료: {font_path}")
                return str(font_path)
            else:
                font_path.unlink(missing_ok=True)
                
        except Exception as e:
            logger.warning(f"❌ 폰트 다운로드 실패 ({i+1}): {e}")
            font_path.unlink(missing_ok=True)
    
    logger.error("❌ 모든 폰트 다운로드 시도 실패")
    return None

def get_korean_font(size=32):  # 폰트 크기 증가
    """한국어 폰트 로드"""
    try:
        # 다운로드한 폰트 시도
        korean_font_path = download_korean_font()
        if korean_font_path and os.path.exists(korean_font_path):
            try:
                font = ImageFont.truetype(korean_font_path, size)
                logger.info(f"✅ 한국어 폰트 사용: {korean_font_path}")
                return font
            except Exception as e:
                logger.warning(f"폰트 로드 실패: {e}")
        
        # 시스템 폰트 시도
        system_fonts = [
            '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
            'C:\\Windows\\Fonts\\malgun.ttf',  # Windows
            '/System/Library/Fonts/AppleSDGothicNeo.ttc',  # macOS
        ]
        
        for font_path in system_fonts:
            if os.path.exists(font_path):
                try:
                    font = ImageFont.truetype(font_path, size)
                    logger.info(f"✅ 시스템 폰트 사용: {font_path}")
                    return font
                except Exception:
                    continue
        
        logger.warning("⚠️ 한국어 폰트를 찾을 수 없어 기본 폰트 사용")
        return ImageFont.load_default()
        
    except Exception as e:
        logger.error(f"폰트 로드 중 오류: {e}")
        return ImageFont.load_default()

def create_qr_with_logo(link_url, output_path, logo_path='static/logo.png', lecture_title=""):
    """개선된 QR 코드 생성 - 중앙 공간 확보, 텍스트 크기 증가"""
    try:
        # QR 코드 생성 (높은 오류 수정 레벨로 중앙 공간 확보)
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,  # 최대 30% 손상 허용
            box_size=15,  # 크기 증가
            border=4,
        )
        qr.add_data(link_url)
        qr.make(fit=True)
        
        # QR 이미지 생성
        qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGBA")
        qr_size = 600  # 크기 증가
        qr_img = qr_img.resize((qr_size, qr_size), Image.LANCZOS)
        
        # 중앙 공간을 흰색으로 채우기 (로고 공간)
        draw_qr = ImageDraw.Draw(qr_img)
        center_size = int(qr_size * 0.3)  # 중앙 30% 공간
        center_pos = (qr_size - center_size) // 2
        draw_qr.rectangle(
            [center_pos, center_pos, center_pos + center_size, center_pos + center_size],
            fill="white"
        )
        
        # 로고 삽입 (있을 경우)
        if os.path.exists(logo_path):
            try:
                logo = Image.open(logo_path).convert("RGBA")
                logo_size = int(qr_size * 0.25)  # 중앙 공간보다 작게
                logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
                
                # 로고 위치 계산
                logo_pos = ((qr_size - logo_size) // 2, (qr_size - logo_size) // 2)
                
                # 로고 합성
                qr_img.paste(logo, logo_pos, logo)
            except Exception as e:
                logger.warning(f"로고 삽입 실패: {e}")
        
        # RGBA를 RGB로 변환 (JPEG 저장을 위해)
        final_img = Image.new('RGB', qr_img.size, 'white')
        final_img.paste(qr_img, (0, 0), qr_img if qr_img.mode == 'RGBA' else None)
        
        # 강의명 텍스트 추가
        if lecture_title.strip():
            # 텍스트 영역 높이 설정
            text_height = 120  # 고정 높이
            margin = 20
            
            # 새 이미지 생성 (QR + 텍스트)
            total_height = qr_size + text_height + margin
            final_with_text = Image.new('RGB', (qr_size, total_height), 'white')
            final_with_text.paste(final_img, (0, 0))
            
            draw = ImageDraw.Draw(final_with_text)
            
            # 폰트 설정 (크기 증가)
            font_size = 36
            font = get_korean_font(font_size)
            
            # 텍스트를 줄로 나누기
            max_width = qr_size - 40  # 여백 고려
            lines = []
            words = lecture_title.split()
            current_line = ""
            
            for word in words:
                test_line = current_line + " " + word if current_line else word
                try:
                    bbox = draw.textbbox((0, 0), test_line, font=font)
                    text_width = bbox[2] - bbox[0]
                except:
                    text_width = len(test_line) * (font_size * 0.7)  # 대략적인 계산
                
                if text_width <= max_width:
                    current_line = test_line
                else:
                    if current_line:
                        lines.append(current_line)
                    current_line = word
            
            if current_line:
                lines.append(current_line)
            
            # 최대 2줄로 제한
            if len(lines) > 2:
                lines = lines[:2]
                lines[1] = lines[1][:25] + "..."
            
            # 텍스트 그리기 (중앙 정렬)
            y_offset = qr_size + margin
            for line in lines:
                try:
                    # 텍스트 크기 계산
                    bbox = draw.textbbox((0, 0), line, font=font)
                    text_width = bbox[2] - bbox[0]
                    text_x = (qr_size - text_width) // 2
                    
                    # 그림자 효과
                    shadow_offset = 2
                    draw.text((text_x + shadow_offset, y_offset + shadow_offset), 
                             line, font=font, fill='lightgray')
                    
                    # 메인 텍스트
                    draw.text((text_x, y_offset), line, font=font, fill='black')
                    
                    y_offset += font_size + 10
                except Exception as e:
                    logger.warning(f"텍스트 렌더링 오류: {e}")
                    # 폴백: 기본 폰트로 시도
                    draw.text((20, y_offset), line, fill='black')
                    y_offset += 30
            
            final_img = final_with_text
        
        # 고품질로 저장
        final_img.save(output_path, quality=95, optimize=True)
        logger.info(f"✅ QR 코드 생성 완료: {output_path}")
        
    except Exception as e:
        logger.error(f"❌ QR 코드 생성 실패: {e}")
        # 최소한의 QR 코드 생성
        simple_qr = qrcode.make(link_url)
        simple_qr.save(output_path)