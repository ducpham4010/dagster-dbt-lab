$env:DAGSTER_HOME = 'D:\aml-devops\mylabs\dagster-dbt\dagster-home'

uv add .\packages\dbt_core_experimental_parser-2.0.5-py3-none-win_amd64.whl

cd ~/Desktop/gitlab/dagster-dbt

# 1. Dagster home dùng tạm của dg dev (chỉ chứa run history local, xóa được)
rm -rf .tmp_dagster_home_*

# 2. Dựng lại venv trong thư mục iCloud không sync (đuôi .nosync)
rm -rf .venv
mkdir .venv.nosync && ln -s .venv.nosync .venv
uv sync

# 3. Cố định DAGSTER_HOME ra ngoài Desktop để iCloud không offload nữa
mkdir -p ~/.dagster_home
export DAGSTER_HOME=/Volumes/Data/.dagster_home   # thêm vào ~/.zshrc cho lâu dài

dg dev
