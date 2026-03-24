"""
JWT 토큰 검증 모듈

Spring Boot 백엔드(monglepick-backend)가 발급한 JWT 토큰을 검증합니다.
동일한 JWT_SECRET 키를 사용하여 토큰의 서명을 확인하고,
페이로드에서 사용자 ID(sub)를 추출합니다.

Spring Boot JWT 토큰 구조:
- Header: {"alg": "HS256", "typ": "JWT"}
- Payload: {"sub": "user_id", "email": "...", "role": "USER", "iat": ..., "exp": ...}
"""

from datetime import datetime, timezone

import jwt
from fastapi import HTTPException, status

from app.config import get_settings


class TokenPayload:
    """
    JWT 토큰 페이로드를 담는 데이터 클래스

    Attributes:
        user_id: 사용자 고유 ID (토큰의 sub 클레임, VARCHAR(50) 문자열)
        email: 사용자 이메일
        role: 사용자 역할 (USER 또는 ADMIN)
        exp: 토큰 만료 시각 (Unix timestamp)
    """

    def __init__(self, user_id: str, email: str, role: str, exp: int):
        self.user_id = user_id
        self.email = email
        self.role = role
        self.exp = exp


def verify_token(token: str) -> TokenPayload:
    """
    JWT 토큰을 검증하고 페이로드를 반환합니다.

    Spring Boot가 발급한 HS256 서명 토큰을 동일한 시크릿으로 검증합니다.
    만료된 토큰, 서명 불일치, 잘못된 형식 등은 모두 401 에러를 반환합니다.

    Args:
        token: "Bearer " 접두어가 제거된 JWT 토큰 문자열

    Returns:
        TokenPayload: 검증된 토큰 페이로드

    Raises:
        HTTPException(401): 토큰이 유효하지 않은 경우
    """
    settings = get_settings()

    try:
        # JWT 디코딩 및 서명 검증
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )

        # sub 클레임에서 사용자 ID 추출 (VARCHAR(50) 문자열 그대로 사용)
        user_id: str | None = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="토큰에 사용자 정보(sub)가 없습니다.",
            )

        # 만료 시각 확인 (PyJWT가 자동 검증하지만 명시적으로도 확인)
        exp = payload.get("exp", 0)
        if datetime.fromtimestamp(exp, tz=timezone.utc) < datetime.now(tz=timezone.utc):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="토큰이 만료되었습니다.",
            )

        return TokenPayload(
            user_id=user_id,
            email=payload.get("email", ""),
            role=payload.get("role", "USER"),
            exp=exp,
        )

    except jwt.ExpiredSignatureError:
        # 토큰 만료
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="토큰이 만료되었습니다. 다시 로그인해주세요.",
        )
    except jwt.InvalidTokenError as e:
        # 서명 불일치, 형식 오류 등
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"유효하지 않은 토큰입니다: {str(e)}",
        )
