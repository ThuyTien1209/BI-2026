# MÔ TẢ METADATA – HỆ THỐNG OLAP
### (Kho Dữ Liệu)

---

> **Loại hệ thống:** OLAP – Online Analytical Processing  
> **Mô hình dữ liệu:** Star Schema (Lược đồ hình sao)

---

## MỤC LỤC

1. [dim\_customers – Bảng chiều Khách hàng](#1-dim_customers--bảng-chiều-khách-hàng)
2. [dim\_products – Bảng chiều Sản phẩm](#2-dim_products--bảng-chiều-sản-phẩm)
3. [dim\_date – Bảng chiều Thời gian](#3-dim_date--bảng-chiều-thời-gian)
4. [dim\_targets\_orders – Bảng chiều Chỉ tiêu Đơn hàng](#4-dim_targets_orders--bảng-chiều-chỉ-tiêu-đơn-hàng)
5. [fact\_order\_lines – Bảng thực tế Chi tiết Đơn hàng](#5-fact_order_lines--bảng-thực-tế-chi-tiết-đơn-hàng)
6. [fact\_orders\_aggregate – Bảng thực tế Tổng hợp Đơn hàng](#6-fact_orders_aggregate--bảng-thực-tế-tổng-hợp-đơn-hàng)

---

## 1. `dim_customers` – Bảng chiều Khách hàng

**Mô tả:** Chứa toàn bộ thông tin về khách hàng trong hệ thống.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `customer_id` | Mã định danh duy nhất của mỗi khách hàng |
| 2 | `customer_name` | Tên của khách hàng |
| 3 | `city` | Thành phố nơi khách hàng đang hoạt động / cư trú |

---

## 2. `dim_products` – Bảng chiều Sản phẩm

**Mô tả:** Chứa toàn bộ thông tin về sản phẩm trong danh mục hàng hóa.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `product_name` | Tên của sản phẩm |
| 2 | `product_id` | Mã định danh duy nhất của mỗi sản phẩm |
| 3 | `category` | Danh mục / phân loại mà sản phẩm thuộc về |

---

## 3. `dim_date` – Bảng chiều Thời gian

**Mô tả:** Chứa thông tin ngày tháng theo nhiều cấp độ (ngày, tháng, tuần trong năm), phục vụ phân tích theo chiều thời gian.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `date` | Ngày cụ thể ở cấp độ ngày |
| 2 | `mmm_yy` | Ngày ở cấp độ tháng (ví dụ: Jan-24, Feb-24) |
| 3 | `week_no` | Số thứ tự tuần trong năm tương ứng với cột `date` |

---

## 4. `dim_targets_orders` – Bảng chiều Chỉ tiêu Đơn hàng

**Mô tả:** Chứa dữ liệu chỉ tiêu giao hàng được phân bổ cho từng khách hàng, dùng để đối chiếu với kết quả thực tế.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `customer_id` | Mã định danh duy nhất của khách hàng |
| 2 | `ontime_target %` | Chỉ tiêu tỷ lệ giao hàng đúng hạn (%) |
| 3 | `infull_target %` | Chỉ tiêu tỷ lệ giao hàng đầy đủ số lượng (%) |
| 4 | `otif_target %` | Chỉ tiêu tỷ lệ OTIF – giao đúng hạn và đủ số lượng (%) |

> **OTIF** = *On Time In Full* – chỉ số tổng hợp đánh giá hiệu suất giao hàng.

---

## 5. `fact_order_lines` – Bảng thực tế Chi tiết Đơn hàng

**Mô tả:** Chứa toàn bộ thông tin về các đơn hàng và từng mặt hàng cụ thể bên trong mỗi đơn, ở cấp độ dòng sản phẩm (order line).

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `order_id` | Mã định danh duy nhất của mỗi đơn hàng |
| 2 | `order_placement_date` | Ngày khách hàng đặt đơn hàng |
| 3 | `customer_id` | Mã định danh khách hàng |
| 4 | `product_id` | Mã định danh sản phẩm |
| 5 | `order_qty` | Số lượng sản phẩm khách hàng yêu cầu được giao |
| 6 | `agreed_delivery_date` | Ngày giao hàng đã thỏa thuận giữa khách hàng và Atliq Mart |
| 7 | `actual_delivery_date` | Ngày thực tế Atliq Mart giao hàng đến khách hàng |
| 8 | `delivered_qty` | Số lượng sản phẩm thực tế đã được giao đến khách hàng |

---

## 6. `fact_orders_aggregate` – Bảng thực tế Tổng hợp Đơn hàng

**Mô tả:** Chứa thông tin tổng hợp các chỉ số **On Time**, **In Full** và **OTIF** được tính toán ở cấp độ đơn hàng theo từng khách hàng.

| STT | Tên cột | Giá trị | Mô tả |
|:---:|---------|---------|-------|
| 1 | `order_id` | — | Mã định danh duy nhất của mỗi đơn hàng |
| 2 | `customer_id` | — | Mã định danh khách hàng |
| 3 | `order_placement_date` | — | Ngày khách hàng đặt đơn hàng |
| 4 | `on_time` | `1` / `0` | `1`: Giao đúng hạn — `0`: Giao trễ hạn |
| 5 | `in_full` | `1` / `0` | `1`: Giao đủ số lượng — `0`: Giao thiếu số lượng |
| 6 | `otif` | `1` / `0` | `1`: Đạt cả hai điều kiện — `0`: Không đạt một trong hai |

---

