
-- ferdy adnan test data analyst 
create database Tes_saya; -- buat database

use Tes_saya;-- gunain databasenya

-- soal no 1 a tentang total qty
select Product , City, -- pilih kolom yang mau di tampilkan 
SUM(Qty) as Total, -- buat jumlah total dari Qty dan di berikan alias Total
dense_rank ()over(partition by Product order by sum(Qty) desc) as rnk -- membuat rank di urutkan dari yang terbesar 
from df -- ambil data dari table df 
group by Product ,City -- di group berdasarkan 1. Product dulu habis itu ke city 
order by Product,Total desc; -- dan di order dari Product dan juga total dengan urutan desc

-- soal 1 b 
select Product, City,
sum(qty) * sum(Price) as Total_annumnt, -- total annumn di bentuk akibat hasil perkalian antara jumlah total dari qty dan jumlah total dari price 
 dense_rank() over (partition by Product order by sum(qty) * sum(Price) desc) as rnk
from df
group by Product ,City
order by Product , Total_annumnt desc;


-- soal no 2 
alter table df modify Order_Date datetime; -- modifikasi type data dari text ke datatime

-- membuat CTE (Common Table Expression) untuk dpat  tanggal transaksi pertama setiap alamat pembeli
with first_transaction as (
    select 
        purchase_address,  -- alamat pembeli
        min(order_date) as first_order_date  -- tanggal transaksi pertama
    from 
        df  
    group by 
        purchase_address  -- kelompokin  berdasarkan alamat pembeli
)

-- mengambil data transaksi pertama dan menghitung jumlah transaksi setelahnya untuk setiap alamat
select 
    df.purchase_address,  -- alamat pembeli
    ft.first_order_date as tanggal_pertama_belanja,  -- tanggal transaksi pertama
    df.qty as qty_pertama_belanja,  -- jumlah produk  transaksi pertama
    df.price * df.qty as amount_pertama_belanja,  -- total harga  transaksi pertama
    (
        -- subquery untuk menghitung jumlah transaksi setelah transaksi pertama
        select count(*)  -- menghitung jumlah transaksi
        from df d2  -- mengambil data dari tabel df (alias d2)
        where d2.purchase_address = df.purchase_address  -- membandingkan alamat pembeli
        and d2.order_date > ft.first_order_date  -- hanya transaksi yang terjadi setelah tanggal transaksi pertama
    ) as jumlah_transaksi_after_pertama  -- menyimpan hasil jumlah transaksi setelahnya
    
from 
    df 
join 
    first_transaction ft on df.purchase_address = ft.purchase_address  -- menggabungkan dengan CTE berdasarkan alamat pembeli
where 
    df.order_date = ft.first_order_date  -- menyaring hanya transaksi pertama untuk setiap alamat
order by 
    df.purchase_address;  -- mengurutkan hasil berdasarkan alamat pembeli




