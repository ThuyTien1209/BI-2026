# MÔ TẢ METADATA – HỆ THỐNG OLTP
### (Cơ Sở Dữ Liệu)

---

> **Loại hệ thống:** OLTP – Online Transaction Processing  
> **Mô hình dữ liệu:** Mô hình quan hệ (Relational Model)


---

## 1. `customers` – Bảng Khách hàng

**Mô tả:** Lưu trữ thông tin cơ bản của khách hàng phục vụ các giao dịch trong hệ thống.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `customer_id` | Mã định danh duy nhất của khách hàng |
| 2 | `customer_name` | Tên khách hàng |
| 3 | `city` | Thành phố của khách hàng |

---

## 2. `products` – Bảng Sản phẩm

**Mô tả:** Lưu trữ thông tin cơ bản về sản phẩm trong hệ thống giao dịch.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `product_id` | Mã định danh duy nhất của sản phẩm |
| 2 | `product_name` | Tên sản phẩm |
| 3 | `category` | Danh mục sản phẩm |
| 4 | `price` | Giá bán sản phẩm |
| 5 | `cost` | Giá vốn hàng bán |

---

## 3. `targets_orders` – Bảng Chỉ tiêu Khách hàng

**Mô tả:** Lưu trữ các chỉ tiêu giao hàng được thiết lập cho từng khách hàng, dùng trong đánh giá hiệu suất vận hành.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `customer_id` | Mã định danh khách hàng |
| 2 | `ontime_target` | Chỉ tiêu tỷ lệ giao hàng đúng hạn |
| 3 | `infull_target` | Chỉ tiêu tỷ lệ giao hàng đầy đủ số lượng |
| 4 | `otif_target` | Chỉ tiêu tỷ lệ OTIF – giao đúng hạn và đủ số lượng |

---

## 4. `orders` – Bảng Đơn hàng

**Mô tả:** Lưu trữ thông tin chính của từng đơn hàng, bao gồm khách hàng đặt hàng, thời điểm đặt và các mốc thời gian giao hàng.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `order_id` | Mã định danh duy nhất của đơn hàng |
| 2 | `customer_id` | Mã định danh khách hàng đặt đơn |
| 3 | `order_placement_date` | Ngày khách hàng đặt đơn hàng |
| 4 | `status` | Trạng thái đơn hàng |
| 5 | `note` | Ghi chú |

---

## 5. `order_lines` – Bảng Chi tiết Đơn hàng

**Mô tả:** Lưu trữ thông tin từng mặt hàng trong một đơn hàng, bao gồm số lượng đặt và số lượng thực tế đã giao.

| STT | Tên cột | Mô tả |
|:---:|---------|-------|
| 1 | `order_line_id` | Mã dòng đơn hàng duy nhất
| 2 | `order_id` | Mã đơn hàng – khóa ngoại liên kết với bảng `orders` |
| 3 | `product_id` | Mã sản phẩm – khóa ngoại liên kết với bảng `products` |
| 4 | `order_qty` | Số lượng sản phẩm được yêu cầu trong đơn hàng |
| 5 | `delivered_qty` | Số lượng sản phẩm thực tế đã được giao |
| 6 | `agreed_delivery_date` | Ngày giao hàng cam kết |
| 7 | `actual_delivery_date` | Ngày giao hàng thực tế |

---

