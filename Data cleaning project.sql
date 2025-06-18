use layoffs_project;
select * from layoffs_staging;

with duplicate_cte AS(
select *,
ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging)
select * from duplicate_cte where row_num > 1;
select * from layoffs_staging where company = 'Casper';
SET SQL_SAFE_UPDATES = 0;


ALTER TABLE layoffs_staging ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;
WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
ORDER BY id
) AS row_num
  FROM layoffs_staging
)
DELETE FROM layoffs_staging
WHERE id IN (
  SELECT id FROM duplicate_cte WHERE row_num > 1
);

WITH duplicate_check AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
           ORDER BY id
         ) AS row_num
  FROM layoffs_staging
)
SELECT * FROM duplicate_check
WHERE row_num > 1;

-- standardizing data
select company, trim(company) from layoffs_staging;
UPDATE layoffs_staging
SET company = TRIM(company);

UPDATE layoffs_staging SET location = TRIM(location);
UPDATE layoffs_staging SET industry = TRIM(industry);
UPDATE layoffs_staging SET stage = TRIM(stage);
UPDATE layoffs_staging SET country = TRIM(country);

select distinct industry
from layoffs_staging
order by 1;
UPDATE layoffs_staging
SET industry = 'crypto'
WHERE industry IN ('cryptocurrency', 'crypto ', 'Crypto');

select * from layoffs_staging where industry like 'crypto%';

select distinct country, trim(trailing '.' from country) from layoffs_staging order by 1;
update layoffs_staging 
set country = trim(trailing '.' from country) where country like 'United States%';

ALTER TABLE layoffs_staging
MODIFY COLUMN date DATE;

UPDATE layoffs_staging
SET date = STR_TO_DATE(date, '%m/%d/%Y');
select * from layoffs_staging where total_laid_off is null AND percentage_laid_off is null;
select * from layoffs_staging where industry is null or industry = '';
select * from layoffs_staging where company = 'Airbnb';


















