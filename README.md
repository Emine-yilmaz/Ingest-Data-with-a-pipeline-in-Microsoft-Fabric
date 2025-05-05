# Ingest Data with a Pipeline in Microsoft Fabric

This lab demonstrates how to ingest data into a Microsoft Fabric lakehouse using a pipeline. You will create a pipeline to copy data from an external HTTP source into the lakehouse's OneLake storage. Subsequently, you will use an Apache Spark notebook to transform this data and load it into a Delta table for analysis.

**Estimated Completion Time:** 45 minutes

**Prerequisites:**

* A Microsoft Fabric trial enabled.
* Access to a web browser.

## Steps

### 1. Create a Workspace

1.  Navigate to the [Microsoft Fabric home page](https://app.fabric.microsoft.com/home?experience=fabric) and sign in with your Fabric credentials.
2.  In the left-hand menu bar, select **Workspaces** (🗇).
3.  Create a **New workspace** with a name of your choice. Ensure you select a licensing mode that includes Fabric capacity (Trial, Premium, or Fabric).
4.  Once the workspace is created and opened, it should be empty.

### 2. Create a Lakehouse

1.  In the left-hand menu bar, select **Create**. Under the **Data Engineering** section, choose **Lakehouse**. Give it a unique name.
    * **Note:** If the **Create** option is not visible, click the ellipsis (**...**) first.
2.  After creation, a new lakehouse with empty **Tables** and **Files** sections will be available.
3.  In the **Explorer** pane on the left, in the **...** menu for the **Files** node, select **New subfolder** and create a subfolder named `new_data`.

### 3. Create a Pipeline

1.  On the **Home** page of your lakehouse, select **Get data** and then **New data pipeline**. Name the new pipeline `Ingest Sales Data`.
2.  If the **Copy Data** wizard doesn’t open automatically, select **Copy Data > Use copy assistant** in the pipeline editor page.

#### 3.1. Configure the Copy Data Activity (Source)

1.  In the **Copy Data** wizard, on the **Choose data source** page, search for `HTTP` and select **HTTP** in the **New sources** section.
2.  In the **Connect to data source** pane, enter the following settings:
    * **URL:** `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv`
    * **Connection:** `Create new connection`
    * **Connection name:** Specify a unique name (e.g., `SalesDataHTTP`)
    * **Data gateway:** `(none)`
    * **Authentication kind:** `Anonymous`
3.  Select **Next**.
4.  Ensure the following settings are selected:
    * **Relative URL:** Leave blank
    * **Request method:** `GET`
    * **Additional headers:** Leave blank
    * **Binary copy:** Unselected
    * **Request timeout:** Leave blank
    * **Max concurrent connections:** Leave blank
5.  Select **Next**.
6.  Wait for the data to be sampled and ensure the following settings are selected:
    * **File format:** `DelimitedText`
    * **Column delimiter:** `Comma (,)`
    * **Row delimiter:** `Line feed (\n)`
    * **First row as header:** Selected
    * **Compression type:** `None`
7.  Select **Preview data** to see a sample of the data, then close the preview and select **Next**.

#### 3.2. Configure the Copy Data Activity (Destination)

1.  On the **Connect to data destination** page, set the following options:
    * **Root folder:** `Files`
    * **Folder path name:** `new_data`
    * **File name:** `sales.csv`
    * **Copy behavior:** `None`
2.  Select **Next**.
3.  Set the following file format options:
    * **File format:** `DelimitedText`
    * **Column delimiter:** `Comma (,)`
    * **Row delimiter:** `Line feed (\n)`
    * **Add header to file:** Selected
    * **Compression type:** `None`
4.  Select **Next**.
5.  On the **Copy summary** page, review the details and select **Save + Run**.
6.  A new pipeline with a **Copy Data** activity will be created. Monitor its status in the **Output** pane below the pipeline designer until it succeeds.
7.  In the left-hand menu, select your lakehouse. In the **Lakehouse explorer** pane, expand **Files** and select the `new_data` folder to verify that `sales.csv` has been copied.

### 4. Create a Notebook

1.  On the **Home** page of your lakehouse, in the **Open notebook** menu, select **New notebook**.
2.  Replace the default code in the first cell with the following variable declaration:

    ```python
    table_name = "sales"
    ```

3.  In the **...** menu for this cell (top-right), select **Toggle parameter cell**.
4.  Add a new code cell below the parameters cell using the **+ Code** button. Add the following code to this new cell:

    ```python
    from pyspark.sql.functions import *

    # Read the new sales data
    df = spark.read.format("csv").option("header","true").load("Files/new_data/*.csv")

    ## Add month and year columns
    df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))

    # Derive FirstName and LastName columns
    df = df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

    # Filter and reorder columns
    df = df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "EmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount"]

    # Load the data into a table
    df.write.format("delta").mode("append").saveAsTable(table_name)
    ```

5.  Verify your notebook looks correct and then use the **▷ Run all** button on the toolbar to execute all cells.
    * **Note:** The first run might take a minute as the Spark pool starts.
6.  Once the notebook run is complete, in the **Lakehouse explorer** pane, in the **...** menu for **Tables**, select **Refresh** and verify that a `sales` table has been created.
7.  In the notebook menu bar, use the ⚙️ **Settings** icon to view notebook settings. Set the **Name** of the notebook to `Load Sales` and close the settings pane.
8.  In the hub menu bar on the left, select your lakehouse. In the **Explorer** pane, refresh the view. Expand **Tables** and select the `sales` table to preview its data.

### 5. Modify the Pipeline

1.  In the hub menu bar on the left, select the `Ingest Sales Data` pipeline you created earlier.
2.  On the **Activities** tab, in the **All activities** list, select **Delete data**. Position the new **Delete data** activity to the left of the **Copy data** activity and connect its **On completion** output to the **Copy data** activity.
3.  Select the **Delete data** activity and in the pane below the design canvas, set the following properties:
    * **General:**
        * **Name:** `Delete old files`
    * **Source:**
        * **Connection:** Your lakehouse
        * **File path type:** `Wildcard file path`
        * **Folder path:** `Files / new_data`
        * **Wildcard file name:** `*.csv`
        * **Recursively:** Selected
    * **Logging settings:**
        * **Enable logging:** Unselected
4.  In the pipeline designer, on the **Activities** tab, select **Notebook** to add a **Notebook** activity to the pipeline.
5.  Select the **Copy data** activity and connect its **On Completion** output to the **Notebook** activity.
6.  Select the **Notebook** activity and in the pane below the design canvas, set the following properties:
    * **General:**
        * **Name:** `Load Sales notebook`
    * **Settings:**
        * **Notebook:** `Load Sales`
    * **Base parameters:** Add a new parameter with the following properties:
        * **Name:** `table_name`
        * **Type:** `String`
        * **Value:** `new_sales`
7.  On the **Home** tab, use the 🖫 **(Save)** icon to save the pipeline. Then use the **▷ Run** button to execute the pipeline and wait for all activities to complete.
    * **Note:** If you encounter the error "Spark SQL queries are only possible in the context of a lakehouse...", open your notebook, select your lakehouse on the left pane, select **Remove all Lakehouses**, and then add it again. Go back to the pipeline designer and run it.
8.  In the hub menu bar on the left, select your lakehouse. In the **Explorer** pane, expand **Tables** and select the `new_sales` table to preview its data. This table was created by the notebook when run by the pipeline.

### 6. Clean Up Resources

1.  If you have finished exploring, you can delete the workspace created for this exercise.
2.  In the left-hand bar, select the icon for your workspace.
3.  Select **Workspace settings**.
4.  In the **General** section, scroll down and select **Remove this workspace**.
5.  Select **Delete** to confirm the deletion.

In this exercise, you successfully implemented a data ingestion pipeline in Microsoft Fabric that copies data from an external source and uses a Spark notebook to transform and load it into a Delta table in your lakehouse.
