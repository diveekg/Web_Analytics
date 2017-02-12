import csv,random
from dateutil.parser import parse
import datetime,time
from collections import OrderedDict


class DataFrame(object):
    @classmethod
    def from_csv(cls, file_path, delimiting_character=',', quote_character='"'):
        """
        Opens a file using the csv module.
        https://docs.python.org/2/library/csv.html
        :param file_path: a string representing the path to the file e.g. ~/Documents/textfile.txt
        :param delimiting_character: a string representing the char(s) that separate columns in a row of data
        :param quote_character: a string for the char(s) that surround values in a column, e.g. "value" -> "
        :return: returns a DataFrame object with the data from the csv file at file_path
        """
        # opens a file in read, universal newline mode and store the file object in infile for this with block
        with open(file_path, 'nU') as infile:
            # create a csv.reader object to process the file and store it in the variable reader
            reader = csv.reader(infile, delimiter=delimiting_character, quotechar=quote_character)

            # creates a variable data and assigns it an empty list
            data = []

            # for each row read (past tense) in from the csv by reader
            for row in reader:
                ##########################################################################
                # -------------------------TASK 2-----------------------------------------#
                ##########################################################################
                row = [y.strip() for y in row]  # for removing leading and trailing spaces
                # append a row (a list) to data
                data.append(row)

            # return an instantiated object of the current class (DataFrame if this is my original code)
            # passing data into the list_of_lists argument
            return cls(list_of_lists=data)
            # end of with block, infile is closed automatically

    def __init__(self, list_of_lists, header=True):
        """
        The __init__ method is called anytime you instantiate the class.
        E.g.
        df = DataFrame(list_of_lists=some_list_of_lists)
        :param self: this argument is implicitly passed, i.e. don't worry about it outside of this class definition.
                    It is the object that you've instantiated.
        :param list_of_lists: a list of lists, namely a list of rows, where each row is a list of values for columns
                                in a dataset
        :param header: a list of strings, where each string is a name of a column (in the same order as the columns)
        """

        # if what was passed into header is True or has a value that is equivalent to false, i.e. bool(header) is True
        if header:  # then do this

            # set the header attribute of this DataFrame object that is being instantiated to the first row of what
            # was passed into list_of_lists
            self.header = list_of_lists[0]
            # set the data attribute of this DataFrame object to all the rows after the first row
            # (remember things start from 0 in python not 1)
            self.data = list_of_lists[1:]
            ##########################################################################
            #-------------------------TASK 1-----------------------------------------#
            ##########################################################################
            check_list = self.header
            result = len(check_list) != len(set(check_list))
            if result:
                raise TypeError('Duplicate detected in header, Please have a look at your data file')
        # if what was passed into header is False or has a value that is equivalent to false, i.e. bool(header) is False
        else:  # then do this
            # set the data attr to list_of_lists (there's no header here)
            self.data = list_of_lists

            # create a variable called generated_header and set it as an empty list
            generated_header = []

            # choose the first row of the data set as a sample row to iterate through the columns, enumerate it so
            # we can keep a count of what index we're at in the row
            for index, column in enumerate(self.data[0]):  # for each index and row in this first row of data
                # append a string that is 'column' concatenated with a string of the current index + 1
                generated_header.append('column' + str(index + 1))

            # set our header attr to this generated_header
            self.header = generated_header

        # we're now outside of the if/else

        # create an empty list called ordered_dict_rows
        ordered_dict_rows = []

        # for each row in self.data
        for row in self.data:
            # for each iteration of the above loop create an empty list called ordered_dict_data
            ordered_dict_data = []

            # for each index and value of this row in self.data
            for index, row_value in enumerate(row):
                # append a tuple to ordered_dict_data that contains the value in header that's at the same index as
                # this value in row
                ordered_dict_data.append((self.header[index], row_value))

            # outside of the inner loop (for index, row_value in enumerate(row))
            # ordered_dict_data now contains a list of tuples

            # create an OrderedDict using ordered_dict_data and assign it to ordered_dict_row
            # now we've converted this row to an OrderedDict!
            ordered_dict_row = OrderedDict(ordered_dict_data)

            # append ordered_dict_row to ordered_dict_rows
            ordered_dict_rows.append(ordered_dict_row)

        # now ordered_dict_rows has all the data from before but each row is an OrderedDict instead of just a list of
        # values
        # assign these to the data attr of this DataFrame object
        self.data = ordered_dict_rows

    def __getitem__(self, item):
        """
        the __getitem__ magic method is called whenenver you use square brackets on an object, e.g.  obj[item]

        :param item: this is the object that is inside of the brackets, e.g. df[item]
        :return: returns different things based on what item is, see below
        """
        # this is for rows only
        # if item is an integer or a slice object
        if isinstance(item, (int, slice)):
            return self.data[item]

        # this is for columns only
        # if item is a string or unicode object
        elif isinstance(item, (str, unicode)):
            return [row[item] for row in self.data]

        # this is for rows and columns
        # if item is a tuple
        elif isinstance(item, tuple):
            if isinstance(item[0], list) or isinstance(item[1], list):

                if isinstance(item[0], list):
                    rowz = [row for index, row in enumerate(self.data) if index in item[0]]
                else:
                    rowz = self.data[item[0]]

                if isinstance(item[1], list):
                    if all([isinstance(thing, int) for thing in item[1]]):
                        return [
                            [column_value for index, column_value in enumerate([value for value in row.itervalues()]) if
                             index in item[1]] for row in rowz]
                    elif all([isinstance(thing, (str, unicode)) for thing in item[1]]):
                        return [[row[column_name] for column_name in item[1]] for row in rowz]
                    else:
                        raise TypeError('What the hell is this?')

                else:
                    return [[value for value in row.itervalues()][item[1]] for row in rowz]
            else:
                if isinstance(item[1], (int, slice)):
                    return [[value for value in row.itervalues()][item[1]] for row in self.data[item[0]]]
                elif isinstance(item[1], (str, unicode)):
                    return [row[item[1]] for row in self.data[item[0]]]
                else:
                    raise TypeError('I don\'t know how to handle this...')

        # only for lists of column names
        elif isinstance(item, list):
            return [[row[column_name] for column_name in item] for row in self.data]

    def get_rows_where_column_has_value(self, column_name, value, index_only=False):
        if index_only:
            return [index for index, row_value in enumerate(self[column_name]) if row_value == value]
        else:
            return [row for row in self.data if row[column_name] == value]
            ###########################################################################
            # -------------------------TASK 4-----------------------------------------#
            ################## function for adding rows################################
    def add_rows(self, list_of_lists):
        if isinstance(list_of_lists, list):
            if len(list_of_lists) == len(df.header):
                list_of_lists=[OrderedDict(zip(df.header,list_of_lists))]
                df.data.append(list_of_lists)

                return "Row Added successfully"

            else:
                raise TypeError('Legnth of entered list is invalid,')
        else:
            raise TypeError('Entered value not a list')


    def add_column(self, list_of_values, column_name):
        if isinstance(list_of_values, list) and isinstance(column_name, list):
            df.header = df.header + [column_name]
            if len(df.data) == len(list_of_values):
               print "try again"

            else:
                raise TypeError("Invalid length of list values,Please a valid list")
        else:
           raise TypeError("Entered list is not a list or column is not a list item")





df = DataFrame.from_csv('SalesJan2009.csv', ',', '"')


####################################################
###########  TASK 4 Test ###########################
####################################################
new_row_added = df.add_rows(['1/5/09 4:10', 'Product1', '1200', 'Mastercard', 'Nicola', 'Roodepoort', 'Gauteng', 'South Africa', '1/5/09 2:33','1/7/09 5:13', '-26.1666667', '27.8666667'])

###