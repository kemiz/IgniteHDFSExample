import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.*;
import java.util.List;

/**
 * Created by kemi on 11/11/2016.
 */
public class HDFSExample {

    private static final String ORG_CACHE = HDFSExample.class.getSimpleName() + "Organizations";
    private static final String PERSON_CACHE = HDFSExample.class.getSimpleName() + "Persons";
    private Ignite ignite;
    private IgniteCache companies;
    private IgniteCache employees;

    public static void main(String args[]){
        HDFSExample example = new HDFSExample();
        example.initIgnite();
    }

    private void initIgnite(){
        ignite = Ignition.start();
        createCompanyCache();
        createEmployeeCache();
        loadData();
        executeSQLJoin();
    }

    private void loadData() {
        try {
            loadCompaniesFromCSV("data/companies.txt");
            loadEmployeesFromCSV("data/employees.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createEmployeeCache() {
        CacheConfiguration cfgEmployeeCache = new CacheConfiguration();
        cfgEmployeeCache.setName(PERSON_CACHE);
        cfgEmployeeCache.setBackups(1);
        cfgEmployeeCache.setCacheMode(CacheMode.PARTITIONED);
        cfgEmployeeCache.setIndexedTypes(Integer.class, Person.class);
        employees = ignite.getOrCreateCache(cfgEmployeeCache);
    }

    private void createCompanyCache() {
        CacheConfiguration cfgCompanyCache = new CacheConfiguration();
        cfgCompanyCache.setName(ORG_CACHE);
        cfgCompanyCache.setBackups(0);
        cfgCompanyCache.setCacheMode(CacheMode.REPLICATED);
        cfgCompanyCache.setIndexedTypes(Integer.class, Organization.class);
        companies = ignite.getOrCreateCache(cfgCompanyCache);
    }

    private void loadCompaniesFromCSV(String filename) throws IOException {
        FileInputStream fis = new FileInputStream(new File(filename));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        String cvsSplitBy = ",";
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            String[] company = line.split(cvsSplitBy);
            companies.put(Integer.parseInt(company[0]), new Organization(Integer.parseInt(company[0]),company[1]));
        }
        br.close();
    }

    private void loadEmployeesFromCSV(String filename) throws IOException {
        FileInputStream fis = new FileInputStream(new File(filename));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        String cvsSplitBy = ",";
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            String[] employee = line.split(cvsSplitBy);
            employees.put(Integer.parseInt(employee[0]), new Person(Integer.parseInt(employee[0]), employee[1], Integer.parseInt(employee[2])));
        }
        br.close();
    }

    private void executeSQLJoin(){
        // Select with join between Person and Organization to
        // get the names of all the employees of a specific organization.
        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select " +
                        "Person.Name from Person, " +
                        "\""+ORG_CACHE+"\".Organization as org " +
                "where "
                        + "Person.orgId = org.id "
                        + "and org.name = ?");

//        SqlQuery joinSql = new SqlQuery (Person.class,
//        "from Person, \"" + ORG_CACHE + "\".Organization as org " +
//                        "where Person.orgId = org.id " +
//                        "and lower(org.name) = lower(?)");
//
//        // Execute queries for find employees for different organizations.
//        Iterable<Entry<Integer, Person>> employeesQuery = employees.query(joinSql.setArgs("Microsoft"));
//        for (Entry<Integer, Person> p : employeesQuery){
//            System.out.println(p.getValue().getName());
//        }

        // Execute the query and obtain the query result cursor.
        try (QueryCursor<List<?>> cursor = employees.query(sql.setArgs("Microsoft"))) {
            for (List<?> row : cursor)
                System.out.println("Person name=" + row.get(0));
        }


        
    }

    private class Organization {
        @QuerySqlField(index = true)
        private Integer id;
        @QuerySqlField
        private String name;

        public Organization(Integer id, String name) {
            this.id = id;
            this.name = name;
        }


        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private class Person {
        @QuerySqlField(index = true)
        private Integer id;
        @QuerySqlField
        private String name;
        @QuerySqlField(index = true)
        private Integer orgId;

        public Person(Integer id, String name, Integer orgId) {
            this.id = id;
            this.name = name;
            this.orgId = orgId;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getOrgId() {
            return orgId;
        }

        public void setOrgId(Integer orgId) {
            this.orgId = orgId;
        }
    }
}
