# 197. Rising Temperature

- ### Intuition
    The problem requires us to find all the dates where the temperature was higher than the previous day's temperature. For each day, we need to compare the current day's temperature with the previous day's temperature, and if the current day's temperature is higher, we return the `id` of that day.

- ### Key Insights:
    1. **Previous Day Comparison**: For each row (representing a day), we need to compare the temperature with the previous day's temperature.
    2. **Date Difference**: The temperature comparison should only be made between consecutive days. This means we need to ensure that the current day’s record is compared with the previous day’s record.
    3. **Efficient Comparison**: Instead of comparing every pair of rows, we focus on comparing each row with the immediately preceding row (based on the `recordDate`).

- ### Approach
    - #### Step 1: **Ordering the Data by Date**
        - The data must be ordered by `recordDate` to ensure that the comparison between consecutive days is meaningful. We need the earlier dates to come before later dates.

    - #### Step 2: **Comparing Consecutive Days**
        - For each row (representing a day), we need to compare the `temperature` of the current row with the `temperature` of the previous row (the one with the immediately earlier `recordDate`).

    - #### Step 3: **Filtering for Higher Temperatures**
        - Once the comparison between the current day's temperature and the previous day's temperature is done, we filter the rows where the current day's temperature is greater than the previous day's temperature.

    - #### Step 4: **Return the IDs**
        - After filtering, we return the `id` of each row where the temperature was higher than the previous day's temperature.

- ### Common Approach (for SQL, PySpark, and Pandas)
    1. **Sorting Data**:
        - SQL: Use `ORDER BY` to ensure the rows are ordered by `recordDate`.
        - PySpark: Use `.sortValues()` to ensure the data is sorted by `recordDate`.
        - Pandas: Use `.sort_values()` to ensure the DataFrame is ordered by `recordDate`.

    2. **Finding Previous Day's Temperature**:
        - SQL: Perform a self-join on the table where each record is joined with the previous day's record, using `DATEDIFF()` to ensure the records represent consecutive days.
        - PySpark: Perform a self-join on the DataFrame with a condition based on the temperature difference and date difference using `datediff()`.
        - Pandas: Use `.shift(1)` to create a new column that holds the temperature of the previous day for each row.

    3. **Comparing Temperatures**:
        - SQL: In the `ON` clause of the join, compare the `temperature` of the current day (`w1.temperature`) with the `temperature` of the previous day (`w2.temperature`).
        - PySpark: After the join, use the condition `w1.temperature > w2.temperature` to filter rows where the current day’s temperature is greater.
        - Pandas: After shifting the temperatures, filter the DataFrame where `temperature > prev_temp` (the previous day's temperature).

    4. **Returning the IDs**:
        - SQL: Select the `id` from the `w1` table after the join.
        - PySpark: Select the `id` from the `w1` DataFrame after applying the condition.
        - Pandas: Select the `id` column of the filtered DataFrame where the temperature condition is satisfied.

- ### Time Complexity Consideration
    - **SQL**: The time complexity is typically dominated by the `JOIN` and `GROUP BY` operations. Assuming the tables have `n` and `m` rows, the complexity is approximately `O(n + m)`, assuming efficient indexing.
    - **PySpark**: The time complexity can be similar to SQL, though it depends on the size of the data and the number of partitions. If the data is large, Spark will perform distributed computation, but the logic remains the same.
    - **Pandas**: The time complexity for pandas is generally `O(n)` for sorting and filtering the DataFrame, where `n` is the number of rows in the DataFrame.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT w1.id FROM Weather w1 JOIN Weather w2
        ON w1.temperature > w2.temperature AND 
        DATEDIFF(w1.recordDate, w2.recordDate) = 1
        ```
    - **PySpark:**
        ```python3 []
        def rising_temperature(weather: sp.DataFrame) -> sp.DataFrame:
            ids_With_Rising_Temp = weather.alias('w1').join(weather.alias('w2'), 
                                                on = ((col('w1.temperature') > col('w2.temperature')) &
                                                    (datediff(col('w1.recordDate'), col('w2.recordDate')) == 1)), 
                                                how = 'inner')\
                                            .select(col('w1.id'))
            return ids_With_Rising_Temp
        ```
    - **Pandas:**
        - **Code 1:**
            ```python3 []
            def rising_temperature(weather: pd.DataFrame) -> pd.DataFrame:
                w1 = weather.copy()
                w2 = weather.copy()
                
                data_With_Increasing_Temp = w1.merge(w2, on = ((w1.temperature > w2.temperature)), 
                                                    how = 'inner')
                
                data_With_Increasing_Temp['DateDiff'] = (pd.to_datetime(data_With_Increasing_Temp.recordDate_x) - 
                                                            pd.to_datetime(data_With_Increasing_Temp.recordDate_y))
                
                data_With_Increasing_Temp['TempDiff'] = (data_With_Increasing_Temp.temperature_x - 
                                                        data_With_Increasing_Temp.temperature_y)
                
                ids_With_Increasing_Temp = data_With_Increasing_Temp[(data_With_Increasing_Temp.DateDiff == '1 Days') & 
                                                                    (data_With_Increasing_Temp.TempDiff > 0)]
                
                ids_With_Increasing_Temp.rename(columns = {'id_x':'id'}, inplace = True)
                
                return ids_With_Increasing_Temp[['id']]
            ```
        - **Code 2:**
            ```python3 []
            def rising_temperature(weather: pd.DataFrame) -> pd.DataFrame:
                # Sort the weather DataFrame by recordDate to ensure proper order
                weather = weather.sort_values(by='recordDate')
                
                # Calculate the difference between consecutive recordDates
                weather['date_diff'] = pd.to_datetime(weather['recordDate']).diff().dt.days
                
                # Use shift() to get the previous day's temperature
                weather['prev_temp'] = weather['temperature'].shift(1)
                
                # Filter for rows where the temperature is greater than the previous day
                # and the date difference is exactly 1 day
                result = weather[(weather['temperature'] > weather['prev_temp']) & 
                                (weather['date_diff'] == 1)]
                
                # Return the 'id' of the rows that meet the condition
                return result[['id']]
            ```