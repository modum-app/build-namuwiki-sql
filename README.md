# build-namuwiki-sql.py

나무위키에서 매월 제공하는 덤프를 바로 [오프라인 리더](https://itunes.apple.com/us/app/namuwiki-offline-reader/id1078563836?mt=8) 등에서 사용 가능한 포맷으로 변환하는 도구.

### 간단 사용법

1. ``git clone https://github.com/modum-app/build-namuwiki-sql.git && cd build-namuwiki-sql``
2. 같은 위치에 [나무위키 공식 덤프](https://namu.wiki/w/나무위키:데이터베이스 덤프)를 내려받기 (7z 권장)
3. ``time 7zr e -so -bd namuwiki180326.7z *.json | python build-namuwiki-sql.py``


### 기타

2018년 3월 26일 덤프 기준으로, 제 컴퓨터에서 변환하는 데 약 90분 걸렸습니다.
* 원본 파일 크기 `8.3G(7z 1.2G)`
* 결과 파일 크기 `1.3G`


### License

GNU GPLv3
Copyright (C) 2016  Yeonwoon JUNG <flow3r@gmail.com>
