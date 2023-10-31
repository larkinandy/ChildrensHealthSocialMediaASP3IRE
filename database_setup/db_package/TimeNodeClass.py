# TimeNodeClass.py 
# Author: Andrew Larkin
# Summary: Perform time-related operations on Neo4j database
# Part of a set of classes for the ASP3IRE project


class TimeDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver
        self.daysInMonth = [31,28,31,30,31,30,31,31,30,31,30,31]

    # create a relatonship between day and month nodes
    # INPUTS:
    #    tx (transaction) - Neo4j driver transaction
    #    month (int) - month of interest
    #    day (int) - day of interest
    #    year (int) - year of interest
    # OUTPUTS:
    #    result - result of Neo4j transaction
    def setWeekDayNode(self,tx,month,day,year):
        from datetime import datetime
        dateString = str(year) + '-' + str(month).zfill(2) + "-" + str(day).zfill(2)
        dt = datetime.strptime(dateString, '%Y-%m-%d')
        code = """MATCH (d:Day {day:$day})--(:Month {month:$month})--(:Year {year:$year})"""
        if(dt.weekday()>4):
            code += """MATCH (w:Weekday {type:'weekend'})"""
        else:
            code += """MATCH (w:Weekday {type:'weekday'})"""

        code += """
            MERGE (d)-[:IS_IN]->(w)
            return d.id
        """
        result = tx.run(code,day=day,month=month,year=year)
        return(result)

    # create relationships between day and month nodes for all days 
    # between start and end years
    # INPUTS:
    #    startYear (int) - start year in time range
    #    endYear (int) - end year in time range
    def connectWeekdayNodes(self,startYear,endYear):
        with self.driver.session() as session:
            for year in range(startYear,endYear+1):
                if(year%4==0):
                    self.daysInMonth[1] = 29
                else:
                    self.daysInMonth[1] = 28
                for month in range(1,13):
                    for day in range(1,self.daysInMonth[month-1]+1):
                        result = session.write_transaction(self.setWeekDayNode,month,day,year)
        return result

        
    

