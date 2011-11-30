<?php
/**
 * Potassium - a convenient PHP library for working with Kasabi
 * 
 */

require 'potassium.inc.php';

/**
 * Potassium is the main class for working with any kasabi datasets or APIs
 *
 * @package Potassium
 */
class Potassium {
  var $_apikey = '';
  var $_responses = array();
  var $_split_n_lines = 10000 ;
  var $_files_to_upload = array();
  private $log ;
  
  function __construct($apikey, $httpconfig = array()) {
    $this->log = new PotassiumLogger() ;
    $this->log->logTrace("Instantiated ".__CLASS__);
    
    $this->_apikey = $apikey;
    $this->_httpconfig = $httpconfig;

  }
  
  /**
   * Call an API and return the results in the most useful format possible
   * 
   * All values passed to the $params argument will be urlencoded.
   * There are potettially limits to the length of parametised URL that can be processed by the server.
   * 
   * @param string $api_name_or_uri The name or URI of a kasabi dataset to which this api call will be directed.
   * @param array $params Specific parameters for the api being requested. e.g. for sparql api array('query' => 'select distinct ?p where {?s ?p ?o .} ')
   * @param string $output Specifies the type of response output required. Defaults to 'json'.
   * @return null if the request was not successful, an array or string otherwise
   */
  function get($api_name_or_uri, $params = array(), $output='json') {
    $this->log->logTrace(__FUNCTION__);

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
  
  /**
   * Post url to the update API
   * 
   * @param string $dataset_name The name of the kasabi dataset to post to.
   * @param string $data_uri The location of the data to get.
   * @return mixed Null if the response was outside a range of 2xx HTTP response codes, otherwise the response body which is an update status URI
   */
  function update_from_uri($dataset_name, $data_uri){
    $this->log->logTrace(__FUNCTION__);

    $uri = 'http://api.kasabi.com/dataset/'.$dataset_name.'/store';
    
    $params = array(  'data-uri' => urlencode($data_uri) , 
                      'apikey' => urlencode($this->_apikey) ) ;
    
    $this->_do_post($uri, $params);

    $response = $this->last_response();
    
    if ($response->responseCode < 200 || $response->responseCode >= 300) {
      return null;
    }else{
      $this->log->logInfo("Submitted uri '{$data_uri}' ");
      $this->_status_uri[] = $response->body ;
      return $response->body ;
    }

    //TODO throw errors?

  }
  
  /**
   * Post a file to a dataset's update API
   * 
   * @param string $dataset_name The name of the kasabi dataset to post to.
   * @param string $file The filename of the data to post.
   * @param string $content_type The content mime_type of the posted data.
   * @return mixed Null if the response was outside a range of 2xx HTTP response codes, otherwise the response body which is an update status URI
   */
  function update_from_file($dataset_name, $file, $content_type){
    $this->log->logTrace(__FUNCTION__);


    $uri = 'http://api.kasabi.com/dataset/'.$dataset_name.'/store'. '?apikey=' . urlencode($this->_apikey);

    // we'll automatically chunk any file into $this->_split_n_lines.
    // note potassium is only safe with ntriples, and certainly not with turtles or water. Severe risk of explosion.
    $this->split_file($file);

    $this->_httpconfig['header'][] = 'Content-Type: '.$content_type ;

    // send each file individually
    if(is_array($this->_files_to_upload)){
        
      foreach ($this->_files_to_upload as $k => $v){
        $this->_do_post($uri, null, file_get_contents($v));
  
        $response = $this->last_response();
      
        if (is_object($response)){
            if ($response->responseCode < 200 || $response->responseCode >= 300) {
              $this->log->logError("Problem uploading file '{$v}' response=\n".print_r($response,true));
              return null;
            }else{
              $this->log->logInfo("Uploaded file '{$v}' ".$response->body);
              $this->_status_uri[] = $response->body ;
              return $response->body ;
            }
        }else{
            $this->log->logError("Problem uploading file '{$v}' response is not an object");
        }
      }
    } else {
        $this->log->logWarn("No files to process");
    }
    //TODO return a sensible status? or just the response body?

  }

  /**
   * Post raw data to a dataset's update API
   * 
   * kasabi is currently limited to only 2MB files, so in potassium we choose to split all data.
   * We reuse a function for splitting files by writing the data to disk and splitting it.
   * This could be optimised to run from memory, especially as the kasabi posts don't take multipart file uploads, 
   *  but just data in the post body.
   * 
   * @param string $dataset_name The name of the kasabi dataset to post to.
   * @param string $file The filename of the data to post.
   * @param string $content_type The content mime_type of the posted data.
   * @return mixed Null if the response was outside a range of 2xx HTTP response codes, otherwise the response body which is an update status URI
   */
  function update_from_data($dataset_name, $data, $content_type){
    $this->log->logTrace(__FUNCTION__);

    $tmp_file = '/tmp/temporary_potassium_data_'.uniqid();
    file_put_contents($tmp_file, $data);  
    $this->update_from_file($dataset_name, $tmp_file, $content_type);
    //TODO some sort of error handling?

    // cleanup
    unlink($tmp_file);
  }

  /**
   * Expand the full api name if not already expanded.
   */
  function api_full_name($api_name_or_uri){
    $this->log->logTrace(__FUNCTION__);
    
    if (strpos($api_name_or_uri, 'http://api.kasabi.com/api/') !== 0) {
      $api_name_or_uri = 'http://api.kasabi.com/api/' . $api_name_or_uri;
    }
    return $api_name_or_uri ;
  }

  /**
   * Get the last HTTP response
   */
  function last_response() {
    $this->log->logTrace(__FUNCTION__);

      if (count($this->_responses)>=1){
        return $this->_responses[count($this->_responses) - 1];
      }
  }

  /**
   * Get the last update status URI
   */
  function last_update_status(){
    $this->log->logTrace(__FUNCTION__);

      if (count($this->_status_uri)>=1){
        return $this->_status_uri[count($this->_status_uri) - 1];
      }
  }

  function is_updated(){
    $this->log->logTrace(__FUNCTION__);
    
    $this->_do_get($this->last_update_status());
    $r = $this->last_response();
    $data = json_decode($r->body,true);
    
    $this->log->logTrace("data=".print_r($data,true));

    
    if ($data['status'] == 'applied' ){
      return true ;
    }else{
      return false ;
    }
  }
  
  /**
   * Make sparql results simpler
   */
  function simplify_sparql_results($results) {
    $this->log->logTrace(__FUNCTION__);

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
  
  /**
   * Guess the format of the returned data if that returned data is returned as JSON
   */
  function guess_format($data) {
    $this->log->logTrace(__FUNCTION__);

    if (isset($data['head']['vars']) && isset($data['results']['bindings'])) {
      return 'sparqlresults';
    }
    return 'unknown';
  }

  /**
   *
   * Split large files into smaller ones
   * 
   * @param string $source Source file
   * @param string $targetpath Target directory for saving files defaults to /tmp/
   * @param int $lines Number of lines to split
   * @return void
   */
  function split_file($source, $targetpath='/tmp/'){
    $this->log->logTrace(__FUNCTION__);

      $processed=0 ;
      $linecounter=0;
      $i=0;
      $j=1;
      $date = date("m-d-y");
      $buffer='';

      $handle = @fopen ($source, "r");
      while (!feof ($handle)) {
          $buffer .= @fgets($handle, 4096);
          $i++;
          $linecounter++;
          if ($i >= $this->_split_n_lines) {
            $processed+=$i;
            $fname = $targetpath.".part_".$date.$j.".log";
            $this->_files_to_upload[] = $targetpath.$fname ;
            $this->log->logTrace("Created part file $targetpath.$fname");
            if (!$fhandle = @fopen($fname, 'w')) {
                $this->log->logError("Cannot open file ($fname)");
                exit;
            }

            if (!@fwrite($fhandle, $buffer)) {
              $this->log->logError("Cannot write to file ($fname)");
              exit;
            }
            fclose($fhandle);
            $j++;
            $buffer='';
            $i=0;
          }
      }
      $this->log->logTrace("linecounter=$linecounter");
      if($linecounter < $this->_split_n_lines){
        $this->_files_to_upload[] = $source;
      }
      
      fclose ($handle);
  }
  

  /**
   * Internal function to perform a HTTP GET request on an URI
   */
  function _do_get($uri) {
    $this->log->logTrace(__FUNCTION__);

    $response = http_parse_message(http_get($uri, $this->_httpconfig));
    $this->_responses[] = $response;
  }

  /**
   * Internal function to perform a HTTP POST request on an URI
   */
  function _do_post($uri, $params=null,  $body=null) {
    $this->log->logTrace(__FUNCTION__);
    $response = http_parse_message(http_post($uri, $params, $body, $this->_httpconfig));
    $this->_responses[] = $response;
  }

}



/**
 * 
 * HTTP helper functions using php curl.
 *
 */

if (!function_exists('http_get')) {
  function http_get($uri, $options) {
    $log = Logger::getLogger('potassium');
    $log->trace(__FUNCTION__);
    
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

    $log->debug("curl_getinfo \$curl_handle=\n".print_r(curl_getinfo($curl_handle),true));

    curl_close($curl_handle);
    return $data;

  }
}

if (!function_exists('http_post')) {
  function http_post($uri, $params=null, $body=null, $options){
    $log = Logger::getLogger('potassium');
    $log->trace(__FUNCTION__);
    
    $curl_handle = curl_init($uri);

    // post count fields
    // post fields
    curl_setopt($curl_handle, CURLOPT_POST, 1);

    $sendparams = '';
    if(!is_null($params)){
      foreach($params as $k =>$v){
        $sendparams .= '&'.$k.'='.$v;
        $sendparams = ltrim($sendparams, '&');
      }
      $log->trace("Setting CURLOPT_POSTFIELDS=$sendparams");
      curl_setopt($curl_handle, CURLOPT_POSTFIELDS, $sendparams);
    }
    
    if(!is_null($body)){
      $log->trace("Setting CURLOPT_POSTFIELDS=$body");
      curl_setopt($curl_handle, CURLOPT_POSTFIELDS, $body);
    }

    // content type
    $log->trace("header=".print_r($options['header'],true));
    curl_setopt($curl_handle, CURLOPT_HTTPHEADER, $options['header']);

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

    //$log->trace("\$curl_handle=".print_r($curl_handle,true));

    $data = curl_exec($curl_handle);

    $log->debug("curl_getinfo \$curl_handle=\n".print_r(curl_getinfo($curl_handle),true));
    
    curl_close($curl_handle);
    
    //$log->trace(__FUNCTION__." \$data ".print_r($data,true));
    return $data;
  }
}

if (!function_exists('http_parse_message')) {
  function http_parse_message($response) {
    $log = Logger::getLogger('potassium');
    $log->trace(__FUNCTION__);
    
    $log->trace(print_r($response,true));

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




/**
 * A simple response object
 */
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
