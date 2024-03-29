package bank_loyal_customer;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//OMOI808692OZ  [{P,Allison,Abbott} {A,1245015582,3667,822,no} {A,1245015582,5806,1035,no} {A,1245015582,1601,635,no} {A,1245015582,4189,802,no} ]

public class BankLoyalReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException,java.lang.InterruptedException
    {
    	
	String fName = "";   
	String lName = "";
	String accountNumber = "";
	int totalDeposit = 0;
	
	Iterator<Text> valuesIter = values.iterator();
	while (valuesIter.hasNext())
	{
		
	    String val = valuesIter.next().toString();   // val    A,1245015582,3667,822,no
	    String [] words = val.split(",");           //  words  [{A} {1245015582} {3667} {822} {no}]
	    if (words[0].equals("P")) 
	    {
		/* customer personal details from PersonMapper */
		fName = words[1];         
		lName = words[2];          
	    }
	    else if (words[0].equals("A"))
	    {
		/* customer account details from AccountMapper */
		accountNumber = words[1];                                    //accountNumber = 1245015582
		/* check for red flag */
		if (words[4].equalsIgnoreCase("yes"))
		{
		    /* 
		     * ignore any other processing since customer cannot be 
		     * loyal if red flag is true for any quarter
		     * No need to output anything for this customer
		     */
		    break;
		}
		/* check for quarter deposit and withdraw */
		int withdrawAmount = Integer.parseInt(words[3]);
		int depositAmount = Integer.parseInt(words[2]);
		if (withdrawAmount >= (depositAmount/2))
		{
		    /* No need to process further */
		    break;
		}
		totalDeposit += depositAmount;                    //totalDeposit  = 3667
	    }
	}
	/* if reached here, all loyal customer conditions met except total deposit amount */
	if (totalDeposit >= 10000)
	{
	    /* id, fName, lName, accountNumber */
	    c.write(key, new Text(fName + "," + lName + "," + accountNumber));
	}
    }
}
