sql_sales_clean = "select * from\
            (select UserId, Pid, ProductName, sale_order_pay_quantity as Quantity, sale_order_gmv as Price, discountamt as Discount, Gender, Age,\
                DeptNameEN as Dept, SubDeptNameEN as SubDept, ClassNameEN as Class, SubClassNameEN as SubClass, BrandName as Brand, OrderId, ShipProvince,\
                TO_DATE(CAST(UNIX_TIMESTAMP(OrderDate, 'MM/dd/yy') AS TIMESTAMP)) as OrderDate, \
                day(TO_DATE(CAST(UNIX_TIMESTAMP(OrderDate, 'MM/dd/yy') AS TIMESTAMP))) as day,\
                month(TO_DATE(CAST(UNIX_TIMESTAMP(OrderDate, 'MM/dd/yy') AS TIMESTAMP))) as month,\
                year(TO_DATE(CAST(UNIX_TIMESTAMP(OrderDate, 'MM/dd/yy') AS TIMESTAMP))) as year\
            from tbSales_2017)\
        where (year = 2017) and (Price > 0) and (Quantity > 0) and (Dept != 'NULL') and (Pid != 'O274117')"

sql_merge_sales_17_18 = """select UserId, OrderDate, Year, Month, OrderId, Gender, Age, Pid, Quantity, Price
        from tbSales_2017
        where Month between 5 and 12
        union
        select UserId, TO_DATE(CAST(concat(Year, '-', Month, '-', Day) AS TIMESTAMP)) as OrderDate, Year, Month, OrderId, Gender, Age, Pid, Quantity, Price
        from tbSales_2018
        where lower(product) not like '%get free%' and UserId is not null"""

sql_top_10_products = """select Level0, concat("[", concat_ws(',', collect_list(Pid)), "]") as Top_Visit
        from
            (select Level0, Pid, Visit_Old, Visit, ROW_NUMBER() over (Partition BY Level0 ORDER BY Visit DESC) AS Rank
            from
                (select Level0, Pid, Visit as Visit_Old,
                        case 
                            when Discount_Rate = 0 then Visit
                            when Discount_Rate between 1 and 24 then Visit * 2
                            when Discount_Rate between 25 and 50 then Visit * 3
                            when Discount_Rate between 51 and 99 then Visit * 4
                        else Visit end as Visit
                from
                    (select Level0, Pid, Visit, cast(((InitialPrice-DiscountPrice)/InitialPrice) * 100 as int) as Discount_Rate
                    from
                        (select distinct a.*, cast(b.InitialPrice as int) as InitialPrice, cast(b.DiscountPrice as Int) as DiscountPrice
                        from
                            (select level0nameen as Level0, Pid, sum(Visit) as Visit
                            from
                                (select *, case when Discount_Rate > 0 then 1 - Discount_Rate else 1 end as Visit
                                from
                                    (select *, cast(((InitialPrice-DiscountPrice)/InitialPrice) as float) as Discount_Rate
                                    from tbSales_bestseller
                                    where InitialPrice > 0 and StockAvailble >= 1))
                            group by Level0, Pid
                            order by Visit desc) a
                        join (select Pid, max(InitialPrice) as InitialPrice, max(DiscountPrice) as DiscountPrice
                            from
                                (select *
                                from tbSales_bestseller
                                where StockAvailble >= 1
                                order by OrderId desc)
                            group by Pid) b 
                        on a.Pid = b.Pid)
                    where ((InitialPrice-DiscountPrice)/InitialPrice) between 0 and 99)))
        where Rank <= 10
        group by Level0"""

sql_top_7_cat_all = """ select Level0, AVG_Discount, ROW_NUMBER() OVER (order by Total_Visit desc) AS Rank,
                    case 
                        when AVG_Discount = 0 then Total_Visit
                        when AVG_Discount between 1 and 24 then Total_Visit * 2
                        when AVG_Discount between 25 and 50 then Total_Visit * 3
                        when AVG_Discount between 51 and 99 then Total_Visit * 4
                    else Total_Visit end as Total_Visit
            from
                (select Level0, count(Pid) as Total_Visit, avg(Discount_Rate) as AVG_Discount
                from
                    (select Level0, Pid, Visit, cast(((InitialPrice-DiscountPrice)/InitialPrice) * 100 as int) as Discount_Rate
                    from
                        (select distinct a.*, cast(b.InitialPrice as int) as InitialPrice, cast(b.DiscountPrice as Int) as DiscountPrice
                        from
                            (select level0nameen as Level0, Pid, sum(Visit) as Visit
                            from
                                (select *, case when Discount_Rate > 0 then 1 - Discount_Rate else 1 end as Visit
                                from
                                    (select *, cast(((InitialPrice-DiscountPrice)/InitialPrice) as float) as Discount_Rate
                                    from tbSales_bestseller
                                    where InitialPrice > 0 and StockAvailble >= 1))
                            group by Level0, Pid
                            order by Visit desc) a
                        join (select Pid, max(InitialPrice) as InitialPrice, max(DiscountPrice) as DiscountPrice
                            from
                                (select *
                                from tbSales_bestseller
                                where StockAvailble >= 1
                                order by OrderId desc)
                            group by Pid) b 
                        on a.Pid = b.Pid)
                    where ((InitialPrice-DiscountPrice)/InitialPrice) between 0 and 99)
                group by Level0)
            order by Total_Visit desc
            limit 7"""

sql_top_7_cat_gender = """select *
        from
            (select initcap(Gender) as Gender, Level0, Total_visit, ROW_NUMBER() over (Partition BY Gender ORDER BY Total_visit DESC) AS Rank
            from
                (select Gender, Level0,
                        case 
                            when AVG_Discount = 0 then Total_Visit
                            when AVG_Discount between 1 and 24 then Total_Visit * 2
                            when AVG_Discount between 25 and 50 then Total_Visit * 3
                            when AVG_Discount between 51 and 99 then Total_Visit * 4
                        else Total_Visit end as Total_Visit
                from
                    (select Gender, Level0, count(Pid) as Total_Visit, avg(Discount_Rate) as AVG_Discount
                    from
                        (select Gender, Level0, Pid, Visit, cast(((InitialPrice-DiscountPrice)/InitialPrice) * 100 as int) as Discount_Rate
                        from
                            (select distinct a.*, cast(b.InitialPrice as int) as InitialPrice, cast(b.DiscountPrice as Int) as DiscountPrice
                            from
                                (select Gender, level0nameen as Level0, Pid, sum(Visit) as Visit
                                from
                                    (select *, case when Discount_Rate > 0 then 1 - Discount_Rate else 1 end as Visit
                                    from
                                        (select *, cast(((InitialPrice-DiscountPrice)/InitialPrice) as float) as Discount_Rate
                                        from tbSales_bestseller
                                        where InitialPrice > 0 and StockAvailble >= 1 and Gender != 'not specify'))
                                group by Gender, Level0, Pid
                                order by Visit desc) a
                            join (select Pid, max(InitialPrice) as InitialPrice, max(DiscountPrice) as DiscountPrice
                                from
                                    (select *
                                    from tbSales_bestseller
                                    where StockAvailble >= 1
                                    order by OrderId desc)
                                group by Pid) b 
                            on a.Pid = b.Pid)
                        where ((InitialPrice-DiscountPrice)/InitialPrice) between 0 and 99)
                    group by Gender, Level0)))
        where Rank <= 7"""

sql_top_7_cat_age = """select *
        from
            (select AgeRange, Level0, Total_visit, ROW_NUMBER() over (Partition BY AgeRange ORDER BY Total_visit DESC) AS Rank
            from
                (select AgeRange, Level0,
                        case 
                            when AVG_Discount = 0 then Total_Visit
                            when AVG_Discount between 1 and 24 then Total_Visit * 2
                            when AVG_Discount between 25 and 50 then Total_Visit * 3
                            when AVG_Discount between 51 and 99 then Total_Visit * 4
                        else Total_Visit end as Total_Visit
                from
                    (select AgeRange, Level0, count(Pid) as Total_Visit, avg(Discount_Rate) as AVG_Discount
                    from
                        (select AgeRange, Level0, Pid, Visit, cast(((InitialPrice-DiscountPrice)/InitialPrice) * 100 as int) as Discount_Rate
                        from
                            (select distinct a.*, cast(b.InitialPrice as int) as InitialPrice, cast(b.DiscountPrice as Int) as DiscountPrice
                            from
                                (select AgeRange, level0nameen as Level0, Pid, sum(Visit) as Visit
                                from
                                    (select *, case when Discount_Rate > 0 then 1 - Discount_Rate else 1 end as Visit
                                    from
                                        (select *, cast(((InitialPrice-DiscountPrice)/InitialPrice) as float) as Discount_Rate,
                                                    case
                                                        when AgeYearsIntRound between 12 and 22 then 'Student'
                                                        when AgeYearsIntRound between 23 and 35 then 'Junior Worker'
                                                        when AgeYearsIntRound between 36 and 50 then 'Senior Worker'
                                                        when AgeYearsIntRound between 51 and 70 then 'Elder'
                                                    end as AgeRange
                                        from tbSales_bestseller
                                        where InitialPrice > 0 and StockAvailble >= 1 and (AgeYearsIntRound between 15 and 70)))
                                group by AgeRange, Level0, Pid
                                order by Visit desc) a
                            join (select Pid, max(InitialPrice) as InitialPrice, max(DiscountPrice) as DiscountPrice
                                from
                                    (select *
                                    from tbSales_bestseller
                                    where StockAvailble >= 1
                                    order by OrderId desc)
                                group by Pid) b 
                            on a.Pid = b.Pid)
                        where ((InitialPrice-DiscountPrice)/InitialPrice) between 0 and 99)
                    group by AgeRange, Level0)))
        where Rank <= 7"""

sql_top_7_cat_gender_age = """select *
        from
            (select initcap(Gender) as Gender, AgeRange, Level0, Total_visit, ROW_NUMBER() over (Partition BY Gender, AgeRange ORDER BY Total_visit DESC) AS Rank
            from
                (select Gender, AgeRange, Level0,
                        case 
                            when AVG_Discount = 0 then Total_Visit
                            when AVG_Discount between 1 and 24 then Total_Visit * 2
                            when AVG_Discount between 25 and 50 then Total_Visit * 3
                            when AVG_Discount between 51 and 99 then Total_Visit * 4
                        else Total_Visit end as Total_Visit
                from
                    (select Gender, AgeRange, Level0, count(Pid) as Total_Visit, avg(Discount_Rate) as AVG_Discount
                    from
                        (select Gender, AgeRange, Level0, Pid, Visit, cast(((InitialPrice-DiscountPrice)/InitialPrice) * 100 as int) as Discount_Rate
                        from
                            (select distinct a.*, cast(b.InitialPrice as int) as InitialPrice, cast(b.DiscountPrice as Int) as DiscountPrice
                            from
                                (select Gender, AgeRange, level0nameen as Level0, Pid, sum(Visit) as Visit
                                from
                                    (select *, case when Discount_Rate > 0 then 1 - Discount_Rate else 1 end as Visit
                                    from
                                        (select *, cast(((InitialPrice-DiscountPrice)/InitialPrice) as float) as Discount_Rate,
                                                    case
                                                        when AgeYearsIntRound between 12 and 22 then 'Student'
                                                        when AgeYearsIntRound between 23 and 35 then 'Junior Worker'
                                                        when AgeYearsIntRound between 36 and 50 then 'Senior Worker'
                                                        when AgeYearsIntRound between 51 and 70 then 'Elder'
                                                    end as AgeRange
                                        from tbSales_bestseller
                                        where InitialPrice > 0 and StockAvailble >= 1 and (Gender != 'not specify') and (AgeYearsIntRound between 15 and 70)))
                                group by Gender, AgeRange, Level0, Pid
                                order by Visit desc) a
                            join (select Pid, max(InitialPrice) as InitialPrice, max(DiscountPrice) as DiscountPrice
                                from
                                    (select *
                                    from tbSales_bestseller
                                    where StockAvailble >= 1
                                    order by OrderId desc)
                                group by Pid) b 
                            on a.Pid = b.Pid)
                        where ((InitialPrice-DiscountPrice)/InitialPrice) between 0 and 99)
                    group by Gender, AgeRange, Level0)))
        where Rank <= 7"""

sql_top_5_cat_demo = """select  AgeRange as Age, Gender, 
        max(CASE WHEN Rank = 1 THEN Top_Visit END) AS Top1,
        max(CASE WHEN Rank = 2 THEN Top_Visit END) AS Top2,
        max(CASE WHEN Rank = 3 THEN Top_Visit END) AS Top3,
        max(CASE WHEN Rank = 4 THEN Top_Visit END) AS Top4,
        max(CASE WHEN Rank = 5 THEN Top_Visit END) AS Top5,
        max(CASE WHEN Rank = 6 THEN Top_Visit END) AS Top6,
        max(CASE WHEN Rank = 7 THEN Top_Visit END) AS Top7
        from
            (select a.Gender, a.AgeRange, a.Rank, b.Top_Visit
            from
                (select 'All' as Gender, 'All' as AgeRange, Rank, Level0
                from tbTop7Cate_All
                union
                select Gender, 'No' as AgeRange, Rank, Level0
                from tbTop7Cate_Gender
                union
                select '0' as Gender, AgeRange, Rank, Level0
                from tbTop7Cate_AgeRange
                union
                select Gender, AgeRange, Rank, Level0
                from tbTop7Cate_GenderAgeRange) a
            join tbTop10Product b on (a.Level0 = b.Level0))
        group by AgeRange, Gender
        order by AgeRange, Gender"""

sql_feq_online = """select UserId, concat("[", concat_ws(',',collect_list(Pid)), "]") as Top_Visit
        from
            (select UserId, concat("'", Pid, "'") as Pid
            from
                (select UserId, Pid, ROW_NUMBER() over (Partition BY UserId ORDER BY Visit DESC) AS Rank
                from
                    (select UserId, Pid, count(Pid) as Visit
                    from tbSales
                    group by UserId, Pid
                    order by Visit desc))
            where Rank <= 15)
        group by UserId"""

sql_feq_offline = """select distinct b.UserId, a.Top_Visit
        from
            (select Cardno, concat("[", concat_ws(',',collect_list(SKUCode)), "]") as Top_Visit
            from
                (select Cardno, concat("'", SKUCode, "'") as SKUCode
                from
                    (select Cardno, SKUCode, ROW_NUMBER() over (Partition BY Cardno ORDER BY Visit DESC) AS Rank
                    from
                        (select Cardno, SKUCode, count(SKUCode) as Visit
                        from tbCDSSales_2018_Offline
                        group by Cardno, SKUCode
                        order by Visit desc))
                where Rank <= 15)
            group by Cardno) a
        join tbUser_2018 b on (a.CardNo = b.t1cardno)"""

sql_non_var_als = """select UserId, Pid, count(Pid) as visit
            from
                (select distinct b.*
                from
                    (select *
                    from
                        (select UserId, count(distinct Pid) as Num_SKU
                        from tbSales
                        group by UserId)
                    where Num_SKU between 1 and 3) a
                join tbSales b on a.UserId = b.UserId)
            group by UserId, Pid"""

sql_var_als = """select UserId, Pid, count(Pid) as visit
            from
                (select distinct b.*
                from
                    (select *
                    from
                        (select UserId, count(distinct Pid) as Num_SKU
                        from tbSales
                        group by UserId)
                    where Num_SKU >= 4) a
                join tbSales b on a.UserId = b.UserId)
            group by UserId, Pid"""

sql_combine_rec = """select distinct c.*, d.Recommend
        from
            (select distinct a.UserId, a.Top_Visit as Frequency_On, b.Top_Visit as Frequency_Off
            from tbFrequency_Online a
            left join tbFrequency_Offline b on a.UserId = b.UserId) c
        left join tbRecommend d on c.UserId = d.UserId"""

sql_personalize = """select UserId, concat("[", concat_ws(',', collect_list(Pid)), "]") as Recommend
        from
            (select distinct a.*, b.UserId, c.Pid
            from
                (select *
                from
                    (select User, Product, Rating, ROW_NUMBER() over (Partition BY User ORDER BY Rating DESC) AS Rank
                    from tbRec)
                where Rank <= 10) a
            left join tbData b on a.User = b.UserIdNew
            left join tbData c on a.Product = c.PidNew
            order by a.User, a.Rank)
        group by UserId"""

sql_best_seller = """
    select *
    from
        (select t1.OrderId, t1.OrderDate, t1.UserId,
            case
                when t4.gender = 1 then 'male'
                when t4.gender = 2 then 'female'
                else 'not specify'
            end as Gender,
            case when t4.birthdate is null then 0 else floor(datediff(to_date(from_unixtime(unix_timestamp())), to_date(t4.birthdate)) / 365.25) end as AgeYearsIntRound,
            t2.Pid, t2.Quantity, t3.StockAvar as StockAvailble, t3.FullPrice as InitialPrice, t2.UnitPriceIncVat as DiscountPrice, t11.*
        from tbOrder t1
        left join tbOrderDetail t2 on t1.OrderId = t2.OrderId
        left join tbProduct t3 on t2.Pid = t3.Pid
        left join tbUser t4 on t1.UserEmail = t4.email
        left join
            (select  pidnew,
                case when level0nameen is null then 'OTHER' else level0nameen end as level0nameen,
                case when level1nameen is null then 'OTHER' else level1nameen end as level1nameen,
                case when level2nameen is null then 'OTHER' else level2nameen end as level2nameen
            from
                (select t1.pidnew, max(level0nameen) as level0nameen, max(level1nameen) as level1nameen, max(level2nameen) as level2nameen
                from tbProduct t1
                left join tbProductGroup t2 on t1.productgroupid = t2.productgroupid
                left join
                    (select *
                    from
                        (select l0.departmentid as level0id,
                            l0.displaynameen as level0nameen,
                            l1.displaynameen as level1nameen,
                            l1.departmentid as level1id,
                            l2.displaynameen as level2nameen,
                            l2.departmentid as level2id
                        from tbDepartment l0
                        left join tbDepartment l1 on l0.departmentid = l1.parentid
                        left join tbDepartment l2 on l1.departmentid = l2.parentid
                        where l0.parentid is null) t
                        union
                        select 0 as level0id,
                            displayname as level0nameen,
                            displayname as level1nameen,
                            0 as level1id,
                            displayname as level2nameen,
                            0 as level1id
                        from tbDepartment l0
                        where parentid is null) t3
                on t2.departmentid = t3.level2id or t2.departmentid = t3.level1id or t2.departmentid = t3.level0id
                where pidnew <> '' or pidnew is not null
                group by t1.pidnew)) t11
        on t2.pidnew = t11.pidnew)
    where datediff(to_date(from_unixtime(unix_timestamp())), to_date(OrderDate)) <= 50 and DiscountPrice > 0 and Quantity > 0 and StockAvailble >= 3
"""

sql_query_click1 = """SELECT fullVisitorId, SKU, sum(Click_Score) as Click_Score
                    FROM
                    (SELECT fullVisitorId, SKU, Click,
                            case
                                when Date_Diff between 1 and 7 then FLOAT(Click) * 2
                                when Date_Diff between 8 and 15 then FLOAT(Click) * 1.8
                                when Date_Diff between 16 and 30 then FLOAT(Click) * 1.4
                                when Date_Diff between 31 and 90 then FLOAT(Click) * 0.5
                            else FLOAT(Click) * 0.2 end as Click_Score
                    FROM
                        (SELECT *, datediff(CURRENT_DATE(), Date) as Date_Diff
                        FROM  
                        (SELECT Date, fullVisitorId, SKU, count(SKU) as Click
                        FROM
                            (SELECT Date, Time, visitNumber, fullVisitorId, SKU
                            FROM
                            (SELECT Date,
                                    visitStartTime as Time,
                                    fullVisitorId,
                                    visitNumber,
                                    hits.transaction.transactionId as TransactionId,
                                    hits.eventInfo.eventAction as Event,
                                    hits.eventInfo.eventLabel as EventLabel,
                                    hits.product.productSKU as SKU
                            FROM TABLE_DATE_RANGE([gap-central-group:38110106.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-04-30')))
                            WHERE Event = 'Product Click'
                            GROUP BY Date, Time, visitNumber, fullVisitorId, SKU)
                        GROUP BY Date, fullVisitorId, SKU)))
                    GROUP BY fullVisitorId, SKU
                    """
                
sql_query_click2 = """SELECT fullVisitorId, SKU, sum(Click_Score) as Click_Score
                    FROM
                    (SELECT fullVisitorId, SKU, Click,
                            case
                                when Date_Diff between 1 and 7 then FLOAT(Click) * 2
                                when Date_Diff between 8 and 15 then FLOAT(Click) * 1.8
                                when Date_Diff between 16 and 30 then FLOAT(Click) * 1.4
                                when Date_Diff between 31 and 90 then FLOAT(Click) * 0.5
                            else FLOAT(Click) * 0.2 end as Click_Score
                    FROM
                        (SELECT *, datediff(CURRENT_DATE(), Date) as Date_Diff
                        FROM  
                        (SELECT Date, fullVisitorId, SKU, count(SKU) as Click
                        FROM
                            (SELECT Date, Time, visitNumber, fullVisitorId, SKU
                            FROM
                            (SELECT Date,
                                    visitStartTime as Time,
                                    fullVisitorId,
                                    visitNumber,
                                    hits.transaction.transactionId as TransactionId,
                                    hits.eventInfo.eventAction as Event,
                                    hits.eventInfo.eventLabel as EventLabel,
                                    hits.product.productSKU as SKU
                            FROM TABLE_DATE_RANGE([gap-central-group:38110106.ga_sessions_], TIMESTAMP('2018-05-01'), TIMESTAMP(CURRENT_DATE())))
                            WHERE Event = 'Product Click'
                            GROUP BY Date, Time, visitNumber, fullVisitorId, SKU)
                        GROUP BY Date, fullVisitorId, SKU)))
                    GROUP BY fullVisitorId, SKU
                    """
                
sql_query_id = """SELECT fullVisitorId, TransactionId
            FROM
              (SELECT Date,
                      fullVisitorId,
                      visitNumber,
                      hits.transaction.transactionId as TransactionId,
                      hits.eventInfo.eventAction as Event,
                      hits.eventInfo.eventLabel as EventLabel,
                      hits.product.productSKU as SKU
              FROM TABLE_DATE_RANGE([gap-central-group:38110106.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP(CURRENT_DATE())))
            WHERE TransactionId is not null
            GROUP BY fullVisitorId, TransactionId"""

sql_order_detail = """select *
        from
          (select distinct a.*, b.Pid, b.TransactionDate
          from
            (select UserId, OrderId
            from tbOrder
            where TransactionDate >= '2018-01-01') a
          join tbOrderDetail b on a.OrderId = b.OrderId)"""

sql_click_behavior = """select UserId, 
                concat("[", concat_ws(',',collect_list(case when Rank between 1 and 10 then SKU end)), "]") as Top10,
                concat("[", concat_ws(',',collect_list(case when Rank between 11 and 20 then SKU end)), "]") as Top11_20
        from
            (select UserId, concat("'", SKU, "'") as SKU, ROW_NUMBER() over (Partition BY UserId ORDER BY Score DESC) AS Rank
            from
                (select UserId, SKU, Score_Click*Score_Purchase as Score
                from
                    (select  UserId, SKU, Score_Click, Recency,
                            case 
                                when Recency is null then 2
                                when Recency >= 91 then 1.8
                                when Recency between 31 and 90 then 1.6
                                when Recency between 16 and 30 then 1
                                when Recency between 1 and 15 then 0.8
                            end as Score_Purchase
                    from
                        (select distinct x.*, y.Recency
                        from
                            (select UserId, SKU as SKU, sum(Click) as Score_Click
                            from
                                (select c.UserId, d.SKU, d.Click
                                from
                                    (select distinct a.UserId, b.FullVisitorId as GAId
                                    from tbUserOrder a
                                    join tbIDOrder b
                                    on a.OrderId = b.TransactionId) c
                                join tbIdClick d on c.GAId = d.FullVisitorId)
                            group by UserId, SKU) x
                        left join 
                            (select UserId, SKU, datediff(current_date, Date) as Recency
                            from
                                (select UserId, Pid as SKU, max(TransactionDate) as Date
                                from tbUserOrder
                                group by UserId, Pid)) y
                        on x.UserId = y.UserId and x.SKU = y.SKU))))
        group by UserId"""