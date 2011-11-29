<?php

require_once 'log4php/Logger.php';

class PotassiumLogger {

    function __CONSTRUCT() {
        
        // create a potassium specific log4php configuration in your own log4php config file.
        // the potassium.logging.properties is not loaded by default.
        $this->logger = Logger::getLogger('potassium');
        //$this->logger->configure($properties);

//print_r($this->logger->getAllAppenders());
//print_r($this->logger);

    }

    function logInfo($message){
        $this->logger->info($message);
    }

    function logError($message){
        $this->logger->error($message);
    }

    function logSelectQuery($request, $query){
        $uri = $request->getUri();
        $message = "SELECT Query:{$uri}:\t<<<{$query}>>>";
        $this->logger->info($message);
        
    }
    function logViewQuery($request, $query){
        $uri = $request->getUri();
        $message = "{$uri}\t<<<{$query}>>>";
        $this->logger->info($message);
    }

    function logDebug($message){
        $this->logger->debug($message);
    }

    function logTrace($message){
        $this->logger->trace($message);
    }

    
}

?>
