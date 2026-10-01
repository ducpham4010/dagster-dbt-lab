sources.yml và schema.yml đều là file YAML khai báo thuộc tính cho dbt. Tách thành hai file chủ yếu để dễ tổ chức; dbt đọc nội dung bên trong, tên file có thể đặt tùy ý. dbt Developer Hub
Theo cách tổ chức thường gặp:
File	Nội dung thường chứa	Ý nghĩa
sources.yml	sources:	Khai báo các bảng nguồn mà dbt đọc
schema.yml	models:	Mô tả, cấu hình và test cho các model của dbt


Mối liên hệ thể hiện qua SQL model. Ví dụ với bảng raw_customers của mày:
1. models/sources.yml khai báo bảng nguồn:
version: 2

sources:
  - name: raw
    tables:
      - name: raw_customers

2. models/staging/stg_customers.sql đọc bảng nguồn và biến đổi dữ liệu:
select
    id as customer_id,
    first_name,
    last_name
from {{ source('raw', 'raw_customers') }}

source('raw', 'raw_customers') tìm source đã khai báo ở bước 1, đồng thời tạo dependency từ source đó đến model stg_customers. dbt Developer Hub
3. models/staging/schema.yml mô tả và khai báo test cho model đó:
version: 2

models:
  - name: stg_customers
    description: "Dữ liệu khách hàng đã chuẩn hóa"
    columns:
      - name: customer_id
        description: "Mã khách hàng"
        data_tests:
          - unique
          - not_null

name: stg_customers khớp với tên file stg_customers.sql, còn customer_id khớp với cột đầu ra của câu SQL. Các test kiểm tra cột này không trùng và không null. dbt Developer Hub
Có hai điểm dễ nhầm:
- Source cũng có thể khai báo columns, mô tả và test, tương tự model. dbt Developer Hub
- Có thể gộp cả sources: và models: vào cùng một schema.yml. dbt vẫn hiểu; việc chia file là lựa chọn tổ chức project. Tên schema.yml cũng không có nghĩa file này tạo database schema.
