/**
 * README
 * Name: EXT006MI.Update
 * Standard Table OOLINE Update
 * Description: Update OOLINE Data
 * Date	    Changed By      	Description
 * 20241123 Hatem Abdellatif - Update OOLINE Data
 * 
 * 20250403 - When called from SDT 'maximum return records' is coming as 1.  
 */
 
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class Update extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
	private final LoggerAPI logger;
	private final ProgramAPI program;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;
  private final ExtensionAPI extension;
  private final InteractiveAPI interactive;
  
  private static final String INTERNAL_PRICE = "I";
  private static final String SALES_PRICE = "S";
  private static final String BOTH_PRICES = "B";
  private static final String INVALID_PRICE_ORIGIN = "Invalid price origin: ";
  private static final String INVALID_OPTION = "Invalid option to choose, please choose 'I' to update the internal price, 'S' to update the sales price or 'B' to update both.";
  
  /**
   * Input Fields of OIS100MI
   */
	private String iCONO;
	private String iORNO;
	private String iPONR;
	private String iPOSX;
	private String iCHOS;
	private String iPRMO;
	
	/**
	 * Input fields ExtendM3Transaction
	 */
	int currentCompany = (Integer)program.getLDAZD().CONO;
	int changeNumber = 0;
	int sequence = 0;
	double currentDiscount = 0;
	String facility = "";
	String warehouse = "";
	String warehouseFacility = "";
	String itemNumber = "";
	String priceList = "";
	String customerNumber = "";
	String orderType = "";
	double internalPrice = 0;
	double correctSalesPrice = 0;
	double givenSalesPrice = 0;
	double discount = 0;
	int validDate = 0;
	List<Double> SalesPriceList = new ArrayList<>();
	List<Integer> dateList = new ArrayList<>();
	// Create a map to store the key-value pairs
  Map<Integer, Double> salesPriceMap = new HashMap<>();
  double requiredSalesPrice = 0;
  
  public Update(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller, ExtensionAPI extension, LoggerAPI logger) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.utility = utility;
    this.miCaller = miCaller;
    this.extension = extension;
    this.logger = logger;
  }
	
  public void main() {
    iCONO = getTrimmedInput("CONO");
    iORNO = getTrimmedInput("ORNO");
    iPONR = getTrimmedInput("PONR");
    iPOSX = getTrimmedInput("POSX");
    iCHOS = getTrimmedInput("CHOS");
    iPRMO = getTrimmedInput("PRMO");
    
    
   
    if (!validate()) {
        return;
    }

    logger.debug("Choice value is " + iCHOS);

    switch (iCHOS.toUpperCase()) {
      case INTERNAL_PRICE:
          handleInternalPrice();
          break;
      case SALES_PRICE:
          handleSalesPrice();
          break;
      case BOTH_PRICES:
          handleBothPrices();
          break;
      default:
          mi.error(INVALID_OPTION);
    }
  }
  
  /**
	 * getTrimmedInput - is a helper function designed to retrieve and trim input values from the mi.inData map.
	 *
	 * @param  key
	 * @return String
	 */
  private String getTrimmedInput(String key) {
      return mi.inData.get(key) == null ? "" : mi.inData.get(key).trim();
  }

  /**
	 * handleInternalPrice - is responsible for managing the internal price update cycle.
	 *
	 * @param  null
	 * @return void
	 */
  private void handleInternalPrice() {
    logger.debug("Inside the internal price cycle.");
    readDiscount();
    saveDiscount();
    getFacilityCustomerOrderTypes();
    retrieveOrderWarehouse();
    retrieveWarehouseFacility();
    readPriceList();
    readInternalPrice();
    requiredSalesPrice = determineSalesPrice();
    logger.debug("Required sales price: " + requiredSalesPrice);
    fetchDiscount();
    updateRecord();
    if (discount != 0) {
        deleteDiscount();
    }
  }
  
  /**
	 * handleSalesPrice - is responsible for managing the sales price update cycle.
	 *
	 * @param  null
	 * @return void
	 */
  private void handleSalesPrice() {
    logger.debug("Inside the update sales price cycle.");
    updatePriceOrigin();
    readDiscount();
    saveDiscount();
    retrieveOrderWarehouse();
    getFacilityCustomerOrderTypes();
    retrieveOrderSalesPrice();
    fetchDiscount();
    
    if (!iPRMO.isEmpty()) {
      if (iPRMO.equals("1") || iPRMO.equals("0")) {
        int waitTime = 1500; // sleep in milliseconds
        int maxRetry = 10;  // Max retries
        int currentRetry = 0;
        double tolerance = 0.0001;
        while (currentRetry <= maxRetry) {
          retrieveOrderWarehouse();
          logger.debug("Counter number: " + currentRetry + ", Given sales price: " + givenSalesPrice);
          if (Math.abs(givenSalesPrice - correctSalesPrice) <= tolerance) {
              logger.debug("If (compare): " + ((givenSalesPrice - correctSalesPrice) <= tolerance));
              logger.debug("Break!");
              break;
          } else {
              logger.debug("Else (compare): " + ((givenSalesPrice - correctSalesPrice) > tolerance));
              updateSalesLine();
          }
          TimeUnit.MILLISECONDS.sleep(waitTime);
          currentRetry++;
        }
      } else {
          mi.error(INVALID_PRICE_ORIGIN + iPRMO + ", please choose (1) to update the price origin or (0) to ignore.");
      }
    }
    updateRecord();
    if (discount != 0) {
        deleteDiscount();
    }
  }

  /**
	 * handleBothPrices - is responsible for managing both internal and sales prices update cycle.
	 *
	 * @param  null
	 * @return void
	 */
  private void handleBothPrices() {
    logger.debug("Inside the internal/update price cycle.");
    updatePriceOrigin();
    readDiscount();
    saveDiscount();
    retrieveOrderWarehouse();
    retrieveWarehouseFacility();
    getFacilityCustomerOrderTypes();
    retrieveOrderSalesPrice();
    readPriceList();
    readInternalPrice();
    requiredSalesPrice = determineSalesPrice();
    fetchDiscount();

    logger.debug("Required sales price: " + requiredSalesPrice);
    if (!iPRMO.isEmpty()) {
      if (iPRMO.equals("1") || iPRMO.equals("0")) {
        int waitTime = 2000; // sleep in milliseconds
        int maxRetry = 10;  // Max retries
        int currentRetry = 0;
        double tolerance = 0.0001;
        while (currentRetry <= maxRetry) {
          retrieveOrderWarehouse();
          logger.debug("Counter number: " + currentRetry + ", Given sales price: " + givenSalesPrice);
          if (Math.abs(givenSalesPrice - correctSalesPrice) <= tolerance) {
              logger.debug("If (compare): " + ((givenSalesPrice - correctSalesPrice) <= tolerance));
              logger.debug("Break!");
              break;
          } else {
              logger.debug("Else (compare): " + ((givenSalesPrice - correctSalesPrice) > tolerance));
              updateSalesLine();
          }
          TimeUnit.MILLISECONDS.sleep(waitTime);
          currentRetry++;
        }
      } else {
          mi.error(INVALID_PRICE_ORIGIN + iPRMO + ", please choose (1) to update the price origin or (0) to ignore.");
      }
    }
    updateRecord();
    if (discount != 0) {
        deleteDiscount();
    }
  }
  
  /**
	 * getFacilityCustomerOrderTypes - to read the facility, customer and order type values in the returned record from standard API OIS100MI.
	 *
	 * @param  null
	 * @return void
	 */
  private void getFacilityCustomerOrderTypes() {
    /**
      * Execute OIS100MI/GetOrderHead
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
        if(response.FACI != null){
          facility = response.FACI;
          customerNumber = response.CUNO;
          orderType = response.ORTP;
        }
    }
    
    /**
     * OIS100MI/GetOrderHead In Data parametrers
     */
    def params = [ "ORNO" : iORNO ]
                   
    logger.debug("Before miCall (OIS100MI.GetOrderHead)");
    miCaller.call("OIS100MI", "GetOrderHead", params, callback);
    logger.debug("Facility value: "   + facility);
    logger.debug("Customer value: "   + customerNumber);
    logger.debug("Order type value: " + orderType);
    logger.debug("After miCall (OIS100MI.GetOrderHead)");
  }
  
  /**
	 * retrieveOrderWarehouse - to read the warehouse value in the returned record from standard API OIS100MI.
	 *
	 * @param  null
	 * @return void
	 */
  private void retrieveOrderWarehouse() {
    /**
      * Execute OIS100MI/GetLine
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
        if(response.WHLO != null){
          warehouse = response.WHLO;
        }
        if(response.ITNO != null){
          itemNumber = response.ITNO;
        }
        if(response.SAPR != null){
          givenSalesPrice = response.SAPR.toDouble();
        }
    }
    
    /**
     * OIS100MI/GetOrderHead In Data parametrers
     */
    def params = [ "CONO" : iCONO, "ORNO" : iORNO, "PONR" : iPONR, "POSX" : iPOSX ]
                   
    logger.debug("Before miCall (OIS100MI.GetLine)");
    miCaller.call("OIS100MI", "GetLine", params, callback);
    logger.debug("Warehouse value: " + warehouse);
    logger.debug("Item number value: " + itemNumber);
    logger.debug("Sales price before update: " + givenSalesPrice);
    logger.debug("After miCall (OIS100MI.GetLine)");
  }
  
  /**
	 * retrieveWarehouseFacility - to read the facility value related to the warehouse in the returned record from standard API MMS005MI.
	 *
	 * @param  null
	 * @return void
	 */
  private void retrieveWarehouseFacility() {
    /**
      * Execute MMS005MI/GetWarehouse
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
        logger.debug("Printing response (0): " + response.toString());
        if(response.FACI != null){
          warehouseFacility = response.FACI;
        }
    }
    
    /**
     * OIS100MI/GetOrderHead In Data parametrers
     */
    def params = [ "WHLO" : warehouse ]
                   
    logger.debug("Before miCall (MMS005MI.GetWarehouse)");
    miCaller.call("MMS005MI", "GetWarehouse", params, callback);
    logger.debug("Warehouse facility value: " + warehouseFacility);
    logger.debug("After miCall (MMS005MI.GetWarehouse)");
  }
  
  /**
	 * readDiscount - to read the discount value in the returned record in OOLINE table.
	 *
	 * @param  null
	 * @return void
	 */
  private void readDiscount() {
    logger.debug("Before miCall (READ)"); 
    int currentCompany = (Integer)program.getLDAZD().CONO;
    DBAction query = database.table("OOLINE")
      .index("00")
      .selection("OBDIP1")
      .build();
    DBContainer container = query.getContainer();
    container.setInt("OBCONO", currentCompany);
    container.set("OBORNO", iORNO);
    container.setInt("OBPONR", iPONR.toInteger());
    container.setInt("OBPOSX", iPOSX.toInteger());
    if (query.read(container)) {
      discount = container.get("OBDIP1");
      logger.debug("Read discount: " + discount);
    }
    logger.debug("After miCall (READ)"); 
  }
  
  /**
	 * readPriceList - to read the pricelist in the returned record in CINPRI table.
	 *
	 * @param  null
	 * @return void
	 */
  private void readPriceList() {
    ExpressionFactory expression = database.getExpressionFactory("CINPRI");
    expression = expression.eq("GBFFAC", facility).and(expression.eq("GBTFAC", warehouseFacility));
    DBAction query = database.table("CINPRI")
      .index("10")
      .matching(expression)
      .selection("GBPRRF", "GBFRDT")
      .build();
      
    DBContainer container = query.getContainer();
    container.setInt("GBCONO", currentCompany);
    int nrOfKeys = 1;
    int nrOfRecords = mi.getMaxRecords() <= 1 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords(); //A 250403 SDT Fazlan
    query.readAll(container, nrOfKeys, nrOfRecords, callback);
  }
  Closure<?> callback = { DBContainer container ->
    logger.debug("Inside if statement: ");
    priceList = container.getString("GBPRRF");
    logger.debug("PriceList value: " + priceList);
  }
  
  /**
	 * readInternalPrice - to read the internal price and latest valid from date in the returned records in OPRBAS table.
	 *
	 * @param  null
	 * @return void
	 */
  private void readInternalPrice() {
    ExpressionFactory expression = database.getExpressionFactory("OPRBAS");
    expression = expression.eq("ODPRRF", priceList).and(expression.eq("ODITNO", itemNumber));
    DBAction query = database.table("OPRBAS")
      .index("10")
      .matching(expression)
      .selection("ODSAPR", "ODVFDT")
      .build();
      
    DBContainer container = query.getContainer();
    container.setInt("ODCONO", currentCompany);
    int nrOfKeys = 1;
    int nrOfRecords = mi.getMaxRecords() <= 1 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();//A 250403 SDT Fazlan
    query.readAll(container, nrOfKeys, nrOfRecords, internalPriceCallback);
    
    logger.debug("Map values: " + salesPriceMap);
    logger.debug("itemNumber currentCompany nrOfRecords" + itemNumber + " " + currentCompany + " " + nrOfRecords);
  }
  Closure<?> internalPriceCallback = { DBContainer container ->
    logger.debug("Inside internal price code block");
    internalPrice = container.getDouble("ODSAPR");
    validDate = container.getInt("ODVFDT");
    salesPriceMap.put(validDate, internalPrice);
  }
  
  /**
	 * updatePriceOrigin - to update the origin price (OBPRMO) into database table OOLINE.
	 *
	 * @param  null
	 * @return void
	 */
  private void updatePriceOrigin() {
    logger.debug("Before miCall (UPDATE PRICE ORIGIN)");
    
    DBAction query = database.table("OOLINE").index("00").build();
    DBContainer container = query.getContainer();
    container.setInt("OBCONO", currentCompany);
    container.set("OBORNO", iORNO);
    container.setInt("OBPONR", iPONR.toInteger());
    container.setInt("OBPOSX", iPOSX.toInteger());
    
    // Update changed information
    if(!query.readLock(container, updateOriginCallBack)){
      mi.error("Record does not exist.");
      return;
    }
    
    logger.debug("After miCall (UPDATE PRICE ORIGIN)");
  }
  
  Closure<?> updateOriginCallBack = { LockedResult lockedResult ->
    
    if (iPRMO != "") {
      if (iPRMO == "1") {
        logger.debug("Updating price origin.");
        lockedResult.set("OBPRMO", "2");
      }
    }
      
	  lockedResult.set("OBLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("YYYYMMdd")).toInteger());
	  lockedResult.set("OBCHNO", lockedResult.getInt("OBCHNO") + 1);
	  lockedResult.set("OBCHID", program.getUser());
    lockedResult.update();
  }
  
  /**
	 * updateRecord - to update the internal transfer price and the discount in the OOLINE table.
	 *
	 * @param  null
	 * @return void
	 */
  private void updateRecord() {
    logger.debug("Before miCall (UPDATE)");
    
    DBAction query = database.table("OOLINE").index("00").build();
    DBContainer container = query.getContainer();
    container.setInt("OBCONO", currentCompany);
    container.set("OBORNO", iORNO);
    container.setInt("OBPONR", iPONR.toInteger());
    container.setInt("OBPOSX", iPOSX.toInteger());
    
    // Update changed information
    if(!query.readLock(container, updateCallBack)){
      mi.error("Record does not exist.");
      return;
    }
    
    logger.debug("After miCall (UPDATE)");
  }
  
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    
    if (iCHOS.toUpperCase() == "I" || iCHOS.toUpperCase() == "B") {
      logger.debug("Updating internal transfer price and discount.");
      lockedResult.setDouble("OBINPR", determineSalesPrice());
      lockedResult.setDouble("OBDIP1", discount);
    } else if (iCHOS.toUpperCase() == "S") {
      logger.debug("Updating sales price and discount." + discount);
      lockedResult.setDouble("OBDIP1", discount);
    }
      
	  lockedResult.set("OBLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("YYYYMMdd")).toInteger());
	  lockedResult.set("OBCHNO", lockedResult.getInt("OBCHNO") + 1);
	  lockedResult.set("OBCHID", program.getUser());
    lockedResult.update();
  }
  
  /**
	 * determineRecentDate - to fetch the most recent valid date with exceeding the current date.
	 *
	 * @param  Map<Integer, Double> salesPriceMap
	 * @return Integer
	 */
  private int determineRecentDate(Map<Integer, Double> salesPriceMap) {
      // Define today's date
      LocalDate today = LocalDate.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
          
      // Convert, filter, and find the most recent date
      Optional<LocalDate> mostRecentDate = salesPriceMap.keySet().stream()
          .map { date -> LocalDate.parse(date.toString(), formatter) }
          .filter { date -> !date.isAfter(today) }
          .max { date1, date2 -> date1.compareTo(date2) }

      // Convert the most recent date back to an integer
      if (mostRecentDate.isPresent()) {
          LocalDate recentDate = mostRecentDate.get();
          String formattedDate = recentDate.format(formatter);
          int recentDateInt = Integer.parseInt(formattedDate);
          logger.debug("Most recent date: " + recentDateInt);
          return recentDateInt;
      } else {
          logger.debug("No valid date found.");
          return 0;
      }
  }
  
  /**
	 * determineSalesPrice - to the required sales price using the valid date.
	 *
	 * @param  null
	 * @return Double
	 */
  private double determineSalesPrice() {
    if (salesPriceMap.size() > 0) {
      return salesPriceMap.get(determineRecentDate(salesPriceMap));
    } else {
      return 0;
    }
  }
  
  /**
	 * saveDiscount - to save the requested discount in a CUSEX table using CUSEXTMI.AddFieldValue
	 *
	 * @param  null
	 * @return Integer
	 */
  private void saveDiscount() {
    /**
      * Execute CUSEXTMI/AddFieldValue
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
    }
    
    /**
     * CUSEXTMI/AddFieldValue In Data parametrers
     */
    def params = [ "FILE" : "OOLINE", "PK01" : iORNO, "PK02" : iPONR.toString(), "A030" : discount.toString() ]
                   
    logger.debug("Before miCall (CUSEXTMI.AddFieldValue)");
    miCaller.call("CUSEXTMI", "AddFieldValue", params, callback);
    logger.debug("Discount saved successfully.");
  }
  
  /**
	 * fetchDiscount - to get the requested discount in a CUSEX table using CUSEXTMI.GetFieldValue.
	 *
	 * @param  null
	 * @return void
	 */
  private void fetchDiscount() {
    /**
      * Execute CUSEXTMI/GetFieldValue
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
        if(response.A030 != null){
          discount = response.A030.toDouble();
          logger.info("Discount value is: " + discount);
        }
    }
    
    /**
     * CUSEXTMI/GetFieldValue In Data parametrers
     */
    def params = [ "FILE" : "OOLINE", "PK01" : iORNO, "PK02" : iPONR.toString() ]
                   
    logger.debug("Before miCall (CUSEXTMI.GetFieldValue)");
    miCaller.call("CUSEXTMI", "GetFieldValue", params, callback);
    logger.debug("Discount fetched successfully.");
  }
  
  /**
	 * deleteDiscount - to delete the requested discount in a CUSEX table using CUSEXTMI.DelFieldValue.
	 *
	 * @param  null
	 * @return void
	 */
  private void deleteDiscount() {
    /**
      * Execute CUSEXTMI/DelFieldValue
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
    }
    
    /**
     * CUSEXTMI/GetFieldValue In Data parametrers
     */
    def params = [ "FILE" : "OOLINE", "PK01" : iORNO, "PK02" : iPONR.toString() ]
                   
    logger.debug("Before miCall (CUSEXTMI.DelFieldValue)");
    miCaller.call("CUSEXTMI", "DelFieldValue", params, callback);
    logger.debug("Discount deleted successfully.");
  }
  
  /**
	 * retrieveOrderSalesPrice - to read the sales price value in the returned record from standard API OIS320MI.
	 *
	 * @param  null
	 * @return void
	 */
  private void retrieveOrderSalesPrice() {
    /**
      * Execute MMS005MI/GetWarehouse
      */
    def callback = {
      Map<String, String> response ->
        logger.info("Response = ${response}");
        logger.debug("Printing response (0): " + response.toString());
        if(response.SAPR != null){
          correctSalesPrice = response.SAPR.toDouble();
        }
    }
    
    /**
     * OIS100MI/GetOrderHead In Data parametrers
     */
    def params = [ "CONO" : currentCompany.toString(), "CUNO" :  customerNumber, "ITNO" : itemNumber, "ORTP" : orderType ]
                   
    logger.debug("Before miCall (OIS320MI.GetPriceLine)");
    miCaller.call("OIS320MI", "GetPriceLine", params, callback);
    logger.debug("Order sales price value: " + correctSalesPrice);
    logger.debug("After miCall (OIS320MI.GetPriceLine)");
  }
  
  /**
	 * updateSalesLine - to run standard API OIS100MI.UpdCOPrice.
	 *
	 * @param  null
	 * @return void
	 */
  private void updateSalesLine() {
    /**
      * Execute OIS100MI/UpdCOPrice
      */
    def callback = {
    Map <String, String> response ->
  
      if (response.error) {
        mi.error(response.errorMessage);
        return;
      }
      
      logger.debug("Response = ${response}");
    }
    
    /**
     * OIS100MI/UpdCOPrice In Data parametrers
     */
    def params = [  "ORNO" : iORNO, "PONR" : iPONR, "POSX" : iPOSX, "RCSP" : "1" ]
                   
    logger.debug("Before miCall (OIS100MI.UpdCOPrice)");               
    miCaller.call("OIS100MI", "UpdCOPrice", params, callback);
    logger.debug("After miCall (OIS100MI.UpdCOPrice)");
  }
  
  /**
	  * validateCompany - Validate comapny
	  *
	  * @param  null
	  * @return boolean
	  */
  private boolean validateCompany() {
    boolean validRecord = false;
    Map<String, String> parameters = ["CONO" : iCONO];
    Closure<Boolean> handler = { Map<String, String> response ->
      if (response.containsKey('errorMsid')){
        validRecord = false;
      } else {
        validRecord = true;
      }
    };
    
    miCaller.call("MNS095MI", "Get", parameters, handler);
    return validRecord;
  }
  
  /**
	 * validate - Validate input
	 *
	 * @param  null
	 * @return boolean
	 */
	private boolean validate() {
	  
	  if (iCONO == "") {
			iCONO = (Integer)program.getLDAZD().CONO;
		} else if (!validateCompany()) {
			mi.error("Company number " + iCONO + " is invalid");
			return false;
		}

		return true;
	}
}