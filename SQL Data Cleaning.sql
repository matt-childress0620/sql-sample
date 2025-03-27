select *
from layoffs
;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns



create table layoffs_staging
like layoffs;

select *
from layoffs_staging;


INSERT layoffs_staging
select *
from layoffs;


select *,
row_number() over(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
from layoffs_staging
;

with duplicate_cte as
(
select *,
row_number() over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage,
 country, funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Oyster'
;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
select *,
row_number() over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage,
 country, funds_raised_millions) AS row_num
from layoffs_staging
;


delete
from layoffs_staging2
where row_num > 1;


select *
from layoffs_staging2;


-- Standaridizing data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2 
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';


select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging2
modify column `date` DATE;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


SELECT *
FROM layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_Staging2
drop column row_num;

update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb';

update layoffs_Staging2
set industry = 'Other'
where company like 'Bally%';