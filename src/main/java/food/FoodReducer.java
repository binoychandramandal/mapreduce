package food;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FoodReducer  extends Reducer<Text, Text, Text, Text>
{
	  // JXJY167254JK [ { 18-06-2017,2,Late delivery}  { 21-06-2017,2,Late delivery} { 20-06-2017,3,Stale food} { 17-07-2017,2,Late delivery }... ]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
    {
                                              //key= month ; value = total orders in that month
	HashMap<Integer, Integer> ordersPerMonth = new HashMap<Integer, Integer>();   // [{ 06 18} {07 7} {08 3} {09 1} ] 
	                                         // feedback , number of feedbacks
	HashMap<String, Integer> feedBacks = new HashMap<String, Integer>();      //  [ {Late delivery  9}  {Stale food  4} {Difficult to order  3}....] 
	
	Iterator<Text> valuesIter = values.iterator();
	/* all data for each customer */
	while (valuesIter.hasNext())
	{
	    String f = valuesIter.next().toString();        // f = 21-06-2017,2,Late delivery
   	    String[] words = f.split(",");                 // words  = [ {21-06-2017} {2} {Late delivery} ]
   	    
	    int month = Integer.parseInt(words[0].split("-")[1].trim());             // month = 06
	    int ratings = Integer.parseInt(words[1].trim());                         // ratings = 2
	    
	    String feedback = words[2].trim();                                       //  feedback  = Late delivery
	    
	    /* count number of orders per month */
	    if (ordersPerMonth.containsKey(month))
	    {
		ordersPerMonth.put(month, ordersPerMonth.get(month) + 1);
	    }
	    else
	    {
		ordersPerMonth.put(month, 1);
	    }
	    if (ratings <= 3)
	    {
		/* count feedback types */
		if (feedBacks.containsKey(feedback))
		{
		    feedBacks.put(feedback, feedBacks.get(feedback) + 1);
		}
		else
		{
		    feedBacks.put(feedback, 1);
		}	    }
	    	}

	System.out.println("*****************************");
	System.out.println(key.toString());
	
	int prevMonthOrders = 0;                              
	int declineCount = 0;
	double orderRate = 0;
	
	for (int i=6; i<=9; ++i)
	{                                                                 // prevMonthOrders = 7
	    Integer currMonthOrders = ordersPerMonth.get(i);            
	                                                                  // currMonthOrders = 3
	   	    
	    if (currMonthOrders == null)
		currMonthOrders = 0;

	    if (prevMonthOrders > 0)
		orderRate = ((1.0*currMonthOrders)/prevMonthOrders)*100;           // orderRate = 0 
	    
	    else
		orderRate = 0;
	    // churn condition
	    if ((currMonthOrders < prevMonthOrders) &&(orderRate <50.0))
	    {
		declineCount++;                                                     // declineCount = 3
	    }
	    
	    else
	    {
		declineCount = 0;
	    }

	    System.out.println(i + ", " + declineCount + ", " + orderRate + ", " + currMonthOrders + ", " + prevMonthOrders);
	    
	    if (declineCount == 3)
	    {
		/* booking is declining for 3 consecutive months */
		String outFeedback = "";
		int feedbackCount = 0;
		for (Map.Entry<String, Integer> e : feedBacks.entrySet())
		{
		    if (e.getValue() > feedbackCount)
		    {
			outFeedback = e.getKey();              
			feedbackCount = e.getValue();
		    }
		}
		c.write(key, new Text(outFeedback));
		//    JXJY167254JK    Late delivery
	    }

	    prevMonthOrders = currMonthOrders;
	}
    }}
