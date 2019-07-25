import requests


class HBaseRestAPI:
    """
    A class used to communicate with the HBase server via it Rest API
    
    '''
    Attributes
    ----------
    __hbase_address: str
        The address of the HBase server database
    
    Methods
    -------
    __get_url(endpoint)
        Get the full url to which a request must be sent
    connect()
        Connect to the HBase server database
    get_tables_list()
        Get the list of tables in the database
    is_table_exists(table_name)
        Test the existence of a table in the database
    put_table_scanner(table_name, batch_size, nb_columns)
        Get the endoint to scan a table
    get_table_content(endpoint)
        Retrieve data related to the endpoint
    '''
    """
    
    def __init__(self, address, port):
        """
        
        Arguments
        ---------
        address: str
            The address of the HBase server database
        port: int
            The port to connect to
        """
        self.__hbase_address = f'http://{address}:{port}'
    
    def __get_url(self, endpoint):
        """
        Get the full url to which a request must be sent
        
        Arguments
        ---------
        endpoint: str
            Link to https://www.cloudera.com/documentation/enterprise/5-9-x/topics/admin_hbase_rest_api.html to see all of endpoints
        
        Returns
        -------
        :str
            The full url
        """
        url = f'{self.__hbase_address}{endpoint}'
        return url
    
    def connect(self):
        """
        Connect to the HBase server database
        
        The function display the version of the cluster if connection successful
        
        Raises
        ------
        :NotImplementedError
            If connection failed
        """
        try:
            response = requests.get(self.__get_url('/version/cluster'))
            print(f'HBase server version: {response.text}')
        except:
            raise NotImplementedError(f'Unable to connect to {self.__hbase_address}.\nVerify connection parameters are ok or HBase Rest API server is up.')
            
    def get_tables_list(self):
        """
        Get the list of tables in the database

        Returns
        -------
        :dict
            List of tables
        """
        response = requests.get(self.__get_url('/'), headers={'Accept': 'application/json'})
        return response.json()
    
    def is_table_exists(self, table_name):
        """
        Test the existence of a table in the database
        
        Arguments
        ---------
        table_name: str
            The name of the table to test its existence
            
        Return
        ------
        :bool
            True if table exists, else False
        """
        response = requests.get(self.__get_url(f'/{table_name}/exists/'))
        return True if response.status_code == 200 else False
    
    def put_table_scanner(self, table_name, xml):
        """
        Get the endoint to scan a table
        
        Arguments
        ---------
        table_name: str
            The name of the table in HBase
        batch: int
            The maximum number of values to return for each call
        nb_columns: int
            The number of column in the table
            
        Raises
        ------
        :NotImplementedError
            If the status code is not 201

        Returns
        -------
        :str
            The endpoint
        """
        response = requests.put(self.__get_url(f'/{table_name}/scanner/'), headers={'Content-Type': 'text/xml'}, data=xml)
        if response.status_code != 201:
            raise NotImplementedError('Error occured with status code {}'.format(response.status_code))
        return response.headers['Location']

    def get_table_content(self, endpoint):
        """
        Retrieve data related to the endpoint

        Arguments
        ---------
        endpoint: str
            The endpoint

        Returns
        -------
        :dict
            Batch (set of rows) of data in the database
        """
        while True:
            response = requests.get(endpoint, headers={'Accept': 'application/json'})
            yield response.json()