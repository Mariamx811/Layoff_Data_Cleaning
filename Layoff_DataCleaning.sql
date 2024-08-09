-- Data Cleaning 
USE world_layoffs;

Select * 
from layoffs;

-- 1- Remove Duplicates. 
-- 2- Standardize the data. 
-- 3- Null values or blank values.
-- 4- Remove any columns (Not should be done on raw data so we make a staging 
-- 	 					 data(Copy the table to another table) to work on. 

CREATE TABLE layoffs_stage 
LIKE layoffs;

SELECT *
FROM layoffs_stage;

SELECT * 
FROM layoffs_stage;

WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(partition by company, location, industry , total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_stage 
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1; 

CREATE TABLE `layoffs_stage2` (
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

INSERT INTO layoffs_stage2
SELECT *,
ROW_NUMBER() OVER(partition by company, location, industry , total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stage ; 

DELETE
From layoffs_stage2
where row_num > 1;

SELECT *
From layoffs_stage2
where row_num > 1;

ALTER TABLE layoffs_stage2
DROP COLUMN row_num;

SELECT *
From layoffs_stage2;

SELECT distinct TRIM(company)
From layoffs_stage2;

select *
from layoffs_stage2
ORDER by industry;

UPDATE layoffs_stage2
set industry = 'Crypto'
where industry LIke 'Crypto%';

Select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_stage2;

update layoffs_stage2
set `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_stage2
MODIFY column `date` DATE;

select distinct country
from layoffs_stage2
order by 1;

update layoffs_stage2
set country = 'United States'
where country like 'United States%';
