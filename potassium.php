<?php
// Potassium - a convenient PHP library for working with Kasabi

class Potassium {
  var $_apikey = '';
  var $_responses = array();
  var $_slpit_n_lines = 10000 ;
  var $_files_to_upload = array();
  
  function __construct($apikey, $httpconfig = array()) {
    $this->_apikey = $apikey;
    $this->_httpconfig = $httpconfig;
  }
  
  // Call an API and return the results in the most useful format possible
  // @return null if the request was not successful, an array or string otherwise
  function get($api_name_or_uri, $params = array(), $output='json') {
       
    $uri = api_full_name($api_name_or_uri) . '?apikey=' . urlencode($this->_apikey) . '&output=' . urlencode($output);

    foreach ($params as $k => $v) {
      if (is_array($v)) $v = join(',', $v);
      $uri .= '&' . $k . '=' . urlencode($v);
    }
    $this->_do_get($uri);
    $response = $this->last_response();
    
    if ($response->responseCode < 200 || $response->responseCode >= 300) {
      return null;
    }
    
    if ($output == 'json') {
      // Automatically convert to array
      $results = json_decode($response->body, true);

      $format = $this->guess_format($results);
      if ($format == 'sparqlresults') {
        // Automatically simplify sparql results format
        return $this->simplify_sparql_results($results);
      }
      else {
        return $results;
      }
      
    }
    else {
      return $response->body;
    }
  }
  
  //post url to the update API
  function update_from_uri($dataset_name, $data_uri){
    $uri = 'http://api.kasabi.com/dataset/'.$dataset_name.'/store';
    
    $params = array(  'data-uri' => urlencode($data_uri) , 
                      'apikey' => urlencode($this->_apikey) ) ;
    
    $this->_do_post($uri, $params);

    $response = $this->last_response();
    
    if ($response->responseCode < 200 || $response->responseCode >= 300) {
      return null;
    }else{
      return $response->body ;
    }

    //TODO throw errors?

  }
  
  // post a file to the update API
  function update_from_file($dataset_name, $file, $content_type){

    $uri = 'http://api.kasabi.com/dataset/'.$dataset_name.'/store';

    // we'll automatically chunk any file into $this->_split_n_lines.
    // note this is only safe with ntriples and nothing will 
    split_file($file);

    $this->_httpconfig['header'][] = 'Content-Type: '.$content_type ;

    // send each file individually
    foreach ($this->_files_to_upload as $k => $v){
      $params['file']="@".$v ;
      $this->_do_post($uri, $params);
    }

  }


  // expand the full api name
  function api_full_name($api_name_or_uri){
     if (strpos($api_name_or_uri, 'http://api.kasabi.com/api/') !== 0) {
      $api_name_or_uri = 'http://api.kasabi.com/api/' . $api_name_or_uri;
    }
    return $api_name_or_uri ;
  }

  function last_response() {
    return $this->_responses[count($this->_responses) - 1];
  }
  
  function simplify_sparql_results($results) {
    $simple_results = array();
    $bindings = $results['results']['bindings'];
    for ($i = 0; $i < count($bindings); $i++) {
      $row = array();
      foreach ($bindings[$i] as $varname => $info) {
        $row[$varname] = $info['value'];
      }
      $simple_results[] = $row;
    }
    return $simple_results;
  }
  
  function guess_format($data) {
    if (isset($data['head']['vars']) && isset($data['results']['bindings'])) {
      return 'sparqlresults';
    }
    return 'unknown';
  }

  /**
   *
   * Split large files into smaller ones
   * @param string $source Source file
   * @param string $targetpath Target directory for saving files
   * @param int $lines Number of lines to split
   * @return void
   */
  function split_file($source, $targetpath='/tmp/'){
      $i=0;
      $j=1;
      $date = date("m-d-y");
      $buffer='';

      $handle = @fopen ($source, "r");
      while (!feof ($handle)) {
          $buffer .= @fgets($handle, 4096);
          $i++;
          if ($i >= $this->_split_n_lines) {
              $fname = $targetpath.".part_".$date.$j.".log";
              $this->_files_to_upload[] = $targetpath.$fname ;
              if (!$fhandle = @fopen($fname, 'w')) {
                  echo "Cannot open file ($fname)";
                  exit;
              }

              if (!@fwrite($fhandle, $buffer)) {
                  echo "Cannot write to file ($fname)";
                  exit;
              }
              fclose($fhandle);
              $j++;
              $buffer='';
              $i=0;
          }
      }
      fclose ($handle);
  }
  
  function _do_get($uri) {
    $response = http_parse_message(http_get($uri, $this->_httpconfig));
    $this->_responses[] = $response;
  }

  function _do_post($uri, $params=null,  $body=null) {
    $response = http_parse_message(http_post($uri, $params, $body, $this->_httpconfig));
    $this->_responses[] = $response;
  }

}

if (!function_exists('http_get')) {
  function http_get($uri, $options) {
    $curl_handle = curl_init($uri);

    curl_setopt($curl_handle, CURLOPT_FRESH_CONNECT,TRUE);
    curl_setopt($curl_handle, CURLOPT_RETURNTRANSFER,1);

    /**
     * @see http://bugs.typo3.org/view.php?id=4292
     */
    if ( !(ini_get('open_basedir')) && ini_get('safe_mode') !== 'On') {
      curl_setopt($curl_handle, CURLOPT_FOLLOWLOCATION, TRUE);
    }

    curl_setopt($curl_handle, CURLOPT_CONNECTTIMEOUT, 5);
    curl_setopt($curl_handle, CURLOPT_TIMEOUT, isset($options['timeout']) ? $options['timeout'] : 600);
    curl_setopt($curl_handle, CURLOPT_HEADER, 1);

    $data = curl_exec($curl_handle);
    curl_close($curl_handle);
    return $data;

  }
}

if (!function_exists('http_post')) {
  function http_post($uri, $params=null, $body=null, $options){
    $curl_handle = curl_init($url);

    // post count fields
    // post fields
    curl_setopt($curl_handle, CURLOPT_POST, 1);
    if(!is_null($params)){    
      $params = join('&',$params);
      curl_setopt($curl_handle, CURLOPT_POSTFIELDS, $params);
    }
    if(!is_null($body)){
      curl_setopt($curl_handle, CURLOPT_POSTFIELDS, $body);
    }

    // content type
    curl_setopt($ch, CURLOPT_HTTPHEADER, $this->_httpconfig['header']);

    curl_setopt($curl_handle, CURLOPT_BINARYTRANSFER, TRUE);
    curl_setopt($curl_handle, CURLOPT_FRESH_CONNECT,TRUE);
    curl_setopt($curl_handle, CURLOPT_RETURNTRANSFER,1);

    /**
     * @see http://bugs.typo3.org/view.php?id=4292
     */
    if ( !(ini_get('open_basedir')) && ini_get('safe_mode') !== 'On') {
      curl_setopt($curl_handle, CURLOPT_FOLLOWLOCATION, TRUE);
    }

    curl_setopt($curl_handle, CURLOPT_CONNECTTIMEOUT, 5);
    curl_setopt($curl_handle, CURLOPT_TIMEOUT, isset($options['timeout']) ? $options['timeout'] : 600);
    curl_setopt($curl_handle, CURLOPT_HEADER, 1);

    $data = curl_exec($curl_handle);
    curl_close($curl_handle);
    return $data;

  }
}

if (!function_exists('http_parse_message')) {
  function http_parse_message($response) {
    do
    {
      if ( strstr($response, "\r\n\r\n") == FALSE) {
        $response_headers = $response;
        $response = '';
      }
      else {
        list($response_headers,$response) = explode("\r\n\r\n",$response,2);
      }
      $response_header_lines = explode("\r\n",$response_headers);

      // first line of headers is the HTTP response code
      $http_response_line = array_shift($response_header_lines);
      if (preg_match('@^HTTP/[0-9]\.[0-9] ([0-9]{3})@',$http_response_line,
      $matches)) {
        $response_code = $matches[1];
      }
      else
      {
        $response_code = "Error";
      }
    }
    while (preg_match('@^HTTP/[0-9]\.[0-9] ([0-9]{3})@',$response));

    $response_body = $response;

    // put the rest of the headers in an array
    $response_header_array = array();
    foreach ($response_header_lines as $header_line) {
      list($header,$value) = explode(': ',$header_line,2);
      $response_header_array[strtolower($header)] = $value;
    }

    $ret = new PotassiumResponse($response_code, $response_header_array, $response_body);
    return $ret;
  }
}

class PotassiumResponse {
  var $responseCode;
  var $headers;
  var $body;
  function __construct($response_code, $headers, $body) {
    $this->responseCode = $response_code;
    $this->headers = $headers;
    $this->body = $body;
  }
}
