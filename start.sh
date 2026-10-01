set -eu
cd "$(dirname "$0")"
export PATH="$HOME/.docker/bin:/usr/local/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"

if ! command -v docker >/dev/null 2>&1; then
    printf 'Не найден Docker. Установи Docker Desktop и повтори запуск.\n'
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    if [ "$(uname -s)" = Darwin ]; then
        open -a Docker
    else
        printf 'Запусти Docker и повтори команду.\n'
        exit 1
    fi
    printf 'Ожидание запуска Docker...\n'
    attempt=0
    until docker info >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 60 ]; then
            printf 'Docker не запустился. Открой Docker Desktop и проверь его окно.\n'
            exit 1
        fi
        sleep 3
    done
fi

docker compose version
mkdir -p data
if [ ! -s data/test.csv ]; then
    archive="${1:-$HOME/Downloads/teta-ml-1-2025.zip}"
    if [ ! -f "$archive" ]; then
        printf 'Не найден %s\nПоложи архив Kaggle в Загрузки или передай путь: bash start.sh /путь/к/архиву.zip\n' "$archive"
        exit 1
    fi
    unzip -p "$archive" test.csv > data/test.csv.tmp
    mv data/test.csv.tmp data/test.csv
fi

if ! docker compose up -d --build; then
    docker compose ps -a
    docker compose logs --tail=60
    exit 1
fi

if ! docker compose run --rm --no-deps check; then
    docker compose ps -a
    docker compose logs --tail=80 scorer db-writer
    exit 1
fi

printf '\nИнтерфейс: http://localhost:8501\nОткрой вкладку Результаты и нажми Посмотреть результаты.\n'
if [ "$(uname -s)" = Darwin ]; then
    open http://localhost:8501
fi
