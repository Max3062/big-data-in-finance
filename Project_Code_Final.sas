%let directory=C:\\Users\\schue\\OneDrive\\Dokumente\\Michigan\\Big Data in Finance\\SAS Project;
libname media "&directory";

*Go through the initial data file from the CRSP dataset and delete all that are not equity funds + all that are "younger" than
5 years + delete all that are dead + delete index and etn funds;

data media.Fund_Info_04;
	set media.All_Funds_Info;
	if PER_COM + PER_PREF + PER_EQ_OTH < 80 then delete;
	if intck('year', FIRST_OFFER_DT, today()) < 5 then delete;
	if DEAD_FLAG = "Y" then delete;
	if INDEX_FUND_FLAG ne "" then delete;
	if ET_FLAG ne "" then delete;
	run;
*Remove all duplicate IDs;

proc sort data=media.Fund_Info_04 out=media.Fund_Info_05 nodupkey;
	by CRSP_FUNDNO;
run;
*From the table containing the information about countries, delete all entries without a WFICN ID because we would
be unable to join;
data media.Fund_Country_info02;
	set media.Fund_Country_info;
	if wficn = "." then delete;
run;

*Perform a left join to the table with the funds from the CSRP data set, use the MFLINK dataset to join the countries from
the Country dataset;
proc sql;
   create table media.Fund_Info_06 as
   select 
      a.*, 
      c.country
   from 
      media.Fund_Info_05 as a
   left join
      media.Mflink_export as b
   on
      a.CRSP_FUNDNO = b.CRSP_FUNDNO
   left join
      media.Fund_Country_Info02 as c
   on
      c.wficn = b.wficn;
quit;
*Delete all the entries (Funds) for which we don't know the country;
data media.Fund_Info_07;
set media.Fund_Info_06;
if country = "" then delete;
run;
*Delete all the unnecessary columns to only keep the basic list of funds;
data media.Funds_Final(keep=CRSP_FUNDNO FUND_NAME FIRST_OFFER_DT NCUSIP);
   set media.Fund_Info_07;
run;
*Based on this create a txt file so that I can extract te holdings data only for the funds I am interested in;
data _null_;
   set media.Funds_Final;
   file 'C:\\Users\\schue\\OneDrive\\Dokumente\\Michigan\\Big Data in Finance\\SAS Project\\ID_List';
   put CRSP_FUNDNO;
run;
*Sort to later join the "previous" quarters;
proc sort data=media.Fund_Holding_Data;
	by crsp_portno ticker report_dt;
run;
*Remove all entries for which the report date is not avilable (necessary for join) and all the entries for which tickers
are empty;
data media.Fund_Holding_Data_cleaned;
    set media.Fund_Holding_Data;

    if report_dt = . or ticker = " " or crsp_portno = . then delete;
run;
*Create a copy of the dataset with a previous quarter column in which all the dates are lagged by 1 to get the previous 
quarter and if there is not "previous" then we put a dot;
data media.Fund_Holding_Lag04;
	set media.Fund_Holding_Data_cleaned;
	by  crsp_portno ticker report_dt;

	format prev_quarter_date date9.;

	prev_quarter_date = lag1(report_dt);

	if first.crsp_portno or first.ticker then prev_quarter_date =.;

	run;
*Create a nw library to store the joined file, in case the file is going to be too big to fit into the already existing
	one;
%let directory=C:\\Users\\schue\\OneDrive\\Dokumente\\Michigan\\Big Data in Finance\\SAS Files;
libname fin "&directory";
*Create a new table that includes the normal dataset plus the previous quarter dates and stock holding percentages by joining
based on the previous quarter_dates;
proc sql;
	create table fin.Fund_Holdings_with_prev as
	select coalesce(a.crsp_portno,b.crsp_portno) as crsp_portno,
		   coalesce(a.ticker,b.ticker) as ticker,
		   coalesce(a.security_name,b.security_name) as security_name,
           max(a.percent_tna,0) as quarter_pct_tna                    , min(a.report_dt,intnx('month',b.report_dt,-3,'e')) as quarter_date,
           max(b.percent_tna,0) as next_q_percent_tna, max(b.report_dt,intnx('month',a.report_dt, 3,'e')) as next_quarter_date
	from media.Fund_Holding_Data_cleaned as a
	full join media.Fund_Holding_Lag04 as b
	on a.crsp_portno = b.crsp_portno and a.ticker = b.ticker and a.report_dt = b.prev_quarter_date;
quit;

*Compute the aboslute difference between the current quarter holdings and the previous quarter holdings percentages;
data media.Fund_Holdings_with_dif;
	set fin.Fund_Holdings_with_prev;
	diff_pct = quarter_pct_tna - next_q_percent_tna;
	diff__pct_abs = abs(quarter_pct_tna - next_q_percent_tna);
run;

proc import out=media.media_export datafile="&directory\\Media_Export.csv" dbms=csv replace;
    getnames=yes;
    datarow=2;
quit;
/*Create table containing all possible month and year combinations during the reporting timeframe*/
data all_month_years;
    start_date = '01JAN1980'd; /* For example, '01JAN2020'd */
    end_date = '31OCT2023'd;     /* For example, '31DEC2023'd */
    do date = start_date to end_date;
        month = month(date);
        year = year(date);
        output;
    end;
    format date yymmdd10.;
    keep month year;
run;

proc sort data=all_month_years nodupkey;
    by month year;
run;
proc sql;
    /* Determine the first mention of each ticker */
    create table ticker_first_mention as
    select
        Ticker,
        min(month(date)) as first_month,
        min(year(date)) as first_year
    from media.Media_Export
    group by Ticker;

	/* Create the final table with all month-year combinations per ticker */
    create table media.monthly_counts as
    select 
        a.Ticker, 
        b.month, 
        b.year, 
        /* Count articles and handle NULL values by setting them to 0 */
        coalesce(sum(case when c.date is not null then 1 else 0 end), 0) as article_count
    from ticker_first_mention as a
    cross join all_month_years as b
    left join media.Media_Export as c
        on a.Ticker = c.Ticker
        and b.month = month(c.date)
        and b.year = year(c.date)
    where
        /* Ensure we only include months after the first mention of each ticker */
        (b.year > a.first_year) or (b.year = a.first_year and b.month >= a.first_month)
    group by a.Ticker, b.month, b.year;
quit;

/* Step 2: Calculate Monthly Averages */
proc sql;
    create table media.monthly_averages as
    select 
        ticker, 
        avg(article_count) as avg_articles
    from media.monthly_counts
    group by ticker;
quit;

/* Make sure to only include tickers that are also in our media data set*/
proc sql;
    create table media.media_filtered_holdings as
    select *
    from media.Fund_Holdings_with_dif
    where ticker in (select distinct ticker from media.final_dataset);
quit;

/*Delete all the entries, for which the quarter date is bigger than the date on which the first article was written */
proc sql;
    create table media.media_filtered_holdings01 as
    select h.*
    from media.media_filtered_holdings as h
    left join ticker_first_mention as t
        on h.Ticker = t.Ticker
    where input(put(h.quarter_date, yymmdd10.), yymmdd10.) >= mdy(t.first_month, 1, t.first_year); /* This condition allows for tickers not in ticker_first_mention to be included */
quit;
/* Merge Counts with Averages and Compute Ratios */
proc sql;
    create table media.final_dataset as
    select 
        a.ticker, 
        a.month, 
        a.year, 
        a.article_count, 
        b.avg_articles,
        (a.article_count / b.avg_articles) as ratio
    from media.monthly_counts as a
    left join media.monthly_averages as b
    on a.ticker = b.ticker;
quit;

/* Ensure final_dataset has proper date format (if not already) */

proc sql;
    create table media.final_dataset_mod as
    select *,
           mdy(month, 1, year) as start_of_month format yymmdd10.,
           intnx('month', mdy(month, 1, year), 0, 'e') as end_of_month format yymmdd10.
    from media.final_dataset;
quit;

proc sql;
    create table media.avg_ratio_over_time01 as
    select 
        h.ticker,
        h.quarter_date,
        h.next_quarter_date,
		h.CRSP_Portno,
		h.diff__pct_abs,
        mean(f.ratio) as avg_ratio
    from 
        media.media_filtered_holdings01 h
    left join 
        media.final_dataset_mod f 
    on 
        h.ticker = f.ticker
    where 
        (f.end_of_month <= h.next_quarter_date and f.start_of_month >= h.quarter_date)
    group by 
       h.CRSP_Portno, h.ticker, h.quarter_date, h.next_quarter_date;
quit;

proc sort data=media.avg_ratio_over_time01 nodupkey;
    by CRSP_Portno ticker quarter_date next_quarter_date;
run;

proc sort data=media.avg_ratio_over_time01; by CRSP_Portno ticker; run;

ods output ParameterEstimates=reg_coef;
proc reg data=media.avg_ratio_over_time01;
    by CRSP_Portno ticker;
    model diff__pct_abs = avg_ratio;
	ods select ParameterEstimates;
run;
ods output close;

proc reg data=media.avg_ratio_over_time01 outset=media.reg_stats01;
    by CRSP_Portno;
    model diff__pct_abs = avg_ratio;
quit;
/*code not required*/
data avg_ratio_coef;
    set reg_coef;
    where variable = 'avg_ratio';
	if ProbT > 0.05 then Estimate = 0;
	keep CRSP_Portno ticker Estimate ProbT;
run;

/* Step 4: Calculate the average of coefficients for each CRSP_Portno CURRENTLY NOT REQUIRED */
proc sql;
    create table media.fund_avg_coef as
    select CRSP_Portno, 
		mean(Estimate) as avg_coef,
        mean(ProbT) as avg_p_value
    from avg_ratio_coef
    group by CRSP_Portno;
quit;

/* Step 1: Sort the dataset by avg_coef descending */
proc sort data=media.reg_stats01 out=media.fund_sorted;
    by descending avg_ratio;
run;
proc rank data = media.reg_stats01 out=media.reg_stats02 percent;
var avg_ratio;
Ranks Average_Ratio_Rank;
Where not missing(_RMSE_);
run;
data media.reg_stats03;
set media.reg_stats02;
if average_ratio_rank <= 33.333333333 then Sensitivity = 0;
If average_ratio_rank <=66.66666666 and average_ratio_rank >33.333333333 then Sensitivity = 1;
if average_ratio_rank > 66.66666666 then Sensitivity = 2;
run;

/* Step 3: Sort by new Class for presentation */
proc sort data=media.fund_classification;
    by Class descending avg_coef;
run;

proc sql;
   create table media.merged_data as 
   select a.CRSP_Portno, 
          a.Sensitivity, 
          b.CRSP_Fundno
   from media.reg_stats03 as a
   inner join media.map_fundno_portno as b
   on a.CRSP_Portno = b.CRSP_Portno;
quit;

proc sql;
   /* Join the monthly returns with the sensitivity data */
   create table media.returns_with_sensitivity as 
   select a.CRSP_Fundno, a.CALDT, a.MRET, b.Sensitivity
   from media.Fund_returns as a
   left join media.merged_data as b
   on a.CRSP_Fundno = b.CRSP_Fundno;

/* Sort the data by CALDT and Sensitivity */
proc sort data=media.returns_with_sensitivity;
   by CALDT Sensitivity;
run;

proc rank data=media.returns_with_sensitivity out=media.ranked_returns percent;
   by CALDT Sensitivity;
   var MRET;
   ranks Rank;
run;

/* Exclude the top and bottom 1% */
data media.filtered_returns;
   set media.ranked_returns;
   if Rank > 1 and Rank <= 99;
run;

/* Calculate the average returns */
proc sql;
   create table media.avg_returns as 
   select CALDT,
          avg(case when Sensitivity = 0 then MRET else . end) as Avg_MRET_Sens_0,
          avg(case when Sensitivity = 1 then MRET else . end) as Avg_MRET_Sens_1,
		  avg(case when Sensitivity = 2 then MRET else . end) as Avg_MRET_Sens_2
   from media.filtered_returns
   group by CALDT;
quit;
quit;

data media.long_format;
    set media.avg_returns;
    format CALDT date9.; /* Assuming CALDT is stored as SAS date value */
    /* Create rows for Sensitivity 0 */
    Sensitivity_Type = "Avg_MRET_Sens_0";
    Average_MRET = Avg_MRET_Sens_0;
    output;
    /* Create rows for Sensitivity 1 */
    Sensitivity_Type = "Avg_MRET_Sens_1";
    Average_MRET = Avg_MRET_Sens_1;
    output;
	Sensitivity_Type = "Avg_MRET_Sens_2";
    Average_MRET = Avg_MRET_Sens_2;
    output;
    keep CALDT Sensitivity_Type Average_MRET; /* Keep only the necessary columns */
run;

proc export data=media.long_format
   outfile="C:\Users\schue\OneDrive\Dokumente\Michigan\Big Data in Finance\SAS Project\Monthly_Returns_Groupv3.csv" /* Specify your desired file path and name */
   dbms=xlsx
   replace;
run;
