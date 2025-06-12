# backend/translation.py
from googletrans import Translator
import time
import logging
from config import SUPPORTED_LANGUAGES

translator = Translator()
logger = logging.getLogger(__name__)

def translate_text(text, target_language):
    """텍스트 번역"""
    try:
        if target_language == 'ko' or not text.strip():
            return text
        
        google_lang_code = target_language
        if target_language == 'zh-cn':
            google_lang_code = 'zh'
        
        result = translator.translate(text, src='ko', dest=google_lang_code)
        translated_text = result.text
        
        logger.info(f"번역 완료: '{text}' → '{translated_text}' ({target_language})")
        return translated_text
        
    except Exception as e:
        logger.warning(f"번역 실패 ({target_language}): {e}, 원본 텍스트 사용")
        return text

def create_multilingual_metadata(korean_text):
    """한국어 텍스트를 모든 지원 언어로 번역"""
    translations = {}
    
    if not korean_text.strip():
        return {lang: '' for lang in SUPPORTED_LANGUAGES.keys()}
    
    for lang_code in SUPPORTED_LANGUAGES.keys():
        try:
            translated = translate_text(korean_text, lang_code)
            translations[lang_code] = translated
            
            if lang_code != 'ko':
                time.sleep(0.2)
                
        except Exception as e:
            logger.error(f"언어 {lang_code} 번역 중 오류: {e}")
            translations[lang_code] = korean_text
    
    return translations