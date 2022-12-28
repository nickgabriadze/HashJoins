

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {

    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }


    public Database() {

    }


    public static Stream<Customer> processInputFileCustomer() throws FileNotFoundException {
        try {
            return new BufferedReader(new FileReader(baseDataDirectory + "/customer.tbl"))
                    .lines()
                    .map(eachString -> eachString.split("\\|"))
                    .map(eachArrayOfString
                            -> new Customer(
                            Integer.parseInt(eachArrayOfString[0]),
                            eachArrayOfString[2].toCharArray(),
                            Integer.parseInt(eachArrayOfString[3]),
                            eachArrayOfString[4].toCharArray(),
                            Float.parseFloat(eachArrayOfString[5]),
                            eachArrayOfString[6],
                            eachArrayOfString[7].toCharArray()
                    ));
        }catch (FileNotFoundException e){
            throw new FileNotFoundException("File not found");
        }
    }


    public static Stream<LineItem> processInputFileLineItem() throws FileNotFoundException {
       try{
           return new BufferedReader(new FileReader(baseDataDirectory + "/lineitem.tbl"))
                   .lines()
                   .map(eachLine -> eachLine.split("\\|"))
                   .map(eachArrayOfString
                                   -> new LineItem(
                                   Integer.parseInt(eachArrayOfString[0]),
                                   Integer.parseInt(eachArrayOfString[1]),
                                   Integer.parseInt(eachArrayOfString[2]),
                                   Integer.parseInt(eachArrayOfString[3]),
                                   Integer.parseInt(eachArrayOfString[4])*100,
                                   Float.parseFloat(eachArrayOfString[5]),
                                   Float.parseFloat(eachArrayOfString[6]),
                                   Float.parseFloat(eachArrayOfString[7]),
                                   eachArrayOfString[8].toCharArray()[0],
                                   eachArrayOfString[9].toCharArray()[0],
                                   LocalDate.parse(eachArrayOfString[10], DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                   LocalDate.parse(eachArrayOfString[11], DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                   LocalDate.parse(eachArrayOfString[12], DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                   eachArrayOfString[13].toCharArray(),
                                   eachArrayOfString[14].toCharArray(),
                                   eachArrayOfString[15].toCharArray()
                           )
                   );

    }catch (FileNotFoundException e){
        throw new FileNotFoundException("File not found");
        }
    }



    public static Stream<Order> processInputFileOrders() throws FileNotFoundException {
      try{
          return new BufferedReader(new FileReader(baseDataDirectory + "/orders.tbl"))
                .lines().map(eachLine -> eachLine.split("\\|"))
                .map(eachArrayOfString -> new Order(
                        Integer.parseInt(eachArrayOfString[0]),
                        Integer.parseInt(eachArrayOfString[1]),
                        eachArrayOfString[2].toCharArray()[0],
                        Float.parseFloat(eachArrayOfString[3]),
                        LocalDate.parse(eachArrayOfString[4],DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                        eachArrayOfString[5].toCharArray(),
                        eachArrayOfString[6].toCharArray(),
                        Integer.parseInt(eachArrayOfString[7]),
                        eachArrayOfString[8].toCharArray()
                ));

    }catch (FileNotFoundException e){
        throw new FileNotFoundException("File not found");
    }
    }



    public long getAverageQuantityPerMarketSegment(String marketsegment) throws FileNotFoundException {
        try {
            List<Integer> custKeysOfMarketSegment = processInputFileCustomer().filter(eachCustomer ->
                    eachCustomer.mktsegment.equals(marketsegment)
            ).map(eachCustKey -> eachCustKey.custKey).toList();

            if(custKeysOfMarketSegment.size() == 0){
                return 0L;
            }

            List<Integer> orderKeys = processInputFileOrders().filter(eachOrder ->
                    custKeysOfMarketSegment.stream().anyMatch(eachCustkey -> eachCustkey.equals(eachOrder.custKey))
            ).map(eachKey -> eachKey.orderKey).toList();


            long[] containingResult = processInputFileLineItem().filter(eachLineItem -> orderKeys.stream().anyMatch(key -> key.equals(eachLineItem.orderKey)))
                    .collect(Collectors.groupingBy(lineItem -> lineItem.orderKey)).values().stream()
                    .map(eachArray -> eachArray.stream().map(each -> each.quantity).toList())
                    .map(eachQuantityArray ->
                            new long[]{(long) eachQuantityArray.size(), eachQuantityArray.stream().mapToLong(eachL -> eachL).summaryStatistics().getSum()})
                    .reduce(new long[2], (firstArray, secondArray) -> new long[]{firstArray[0] + secondArray[0], firstArray[1] + secondArray[1]});

            return containingResult[1] / containingResult[0];

        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("File not found");
        }
    }



    public static void main(String[] args) throws FileNotFoundException {
        System.out.println(new Database().getAverageQuantityPerMarketSegment("MACHINERY"));

    }
}
