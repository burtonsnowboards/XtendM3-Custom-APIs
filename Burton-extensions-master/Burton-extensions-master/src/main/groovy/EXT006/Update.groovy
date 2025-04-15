/**
 * README
 * Name: EXT001MI.Update
 * Standard Table CINACC Update
 * Description: Update CINACC Data
 * Date	    Changed By      	Description
 * 20230601 Hatem Abdellatif - Mohamed Adel  Update CINACC Data
 */
 
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
 
public class Update extends ExtendM3Transaction {
    private final MIAPI mi;
    private final DatabaseAPI database;
	private final LoggerAPI logger;
	private final ProgramAPI program;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;
	
	public Update(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
        this.mi = mi;
        this.database = database;
        this.program = program;
        this.utility = utility;
    }


	/**
	 * Input fields ExtendM3Transaction
	 */
	private String iCONO;
	private String iDIVI;
	private String iANBR;
	private String iSENO;
	private String iERCD;
	int currentCompany = (Integer)program.getLDAZD().CONO;
	String currentDivision = program.LDAZD.DIVI.toString();
	int changeNumber = 0;
	int sequence = 0;

  
    public void main() {
        iCONO = mi.inData.get("CONO") == null ? "" : mi.inData.get("CONO").trim();
        iDIVI = mi.inData.get("DIVI") == null ? "" : mi.inData.get("DIVI").trim();
        iANBR = mi.inData.get("ANBR") == null ? "" : mi.inData.get("ANBR").trim();
        iSENO = mi.inData.get("SENO") == null ? "" : mi.inData.get("SENO").trim();
        iERCD = mi.inData.get("ERCD") == null ? "" : mi.inData.get("ERCD").trim();

        //If no error then update the record
        changeRecord();
    }
  
  
    /**
    * Updates record in the CINACC table.
    * by updating the account number, sequence number and error type value.
    *
    * @param  null
    * @return void
    */
    private void changeRecord() {
        DBAction query = database.table("CINACC").index("00").selection("EZCONO", "EZDIVI", "EZANBR", "EZSENO", "EZERCD").build();
        DBContainer container = query.getContainer();

        container.setInt("EZCONO", currentCompany);
        container.set("EZDIVI", currentDivision);
        container.setLong("EZANBR", iANBR.toLong());
        container.setInt("EZSENO", iSENO.toInteger());
        //mi.error("EZSENO: " + iSENO);
        //container.set("EZERCD", iERCD);
        query.readLock(container, updateCallBack);

        // Update changed information
        if(!query.readLock(container, updateCallBack)){
            mi.error("Record does not exist");
            return;
        }

    }
  
    Closure<?> updateCallBack = { LockedResult lockedResult ->
        lockedResult.set("EZLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("YYYYMMdd")).toInteger());
        lockedResult.set("EZCHNO", (changeNumber + 1));
        lockedResult.set("EZCHID", program.getUser());
        lockedResult.setInt("EZCONO", currentCompany);
        lockedResult.set("EZDIVI", currentDivision);
        lockedResult.setLong("EZANBR", iANBR.toLong());
        lockedResult.setInt("EZSENO", iSENO.toInteger());
        lockedResult.set("EZERCD", iERCD);
        lockedResult.update();
    }
  
}