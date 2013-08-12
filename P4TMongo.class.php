<?
    /***
     *
     *  P4TMongo is a PHP Class that mimics the popular AdoDB PHP Library. 
     *  Use your favorite getOne, getRow, getAll, getCol and other commands with MongoDB
     *  
     *  @author     Alexey Kulikov <a.kulikov@gmail.com>
     *  @copyright  POOL4TOOL AG, Vienna, Austria
     *  @version    1.2
     *
     ***/
    class P4TMongo extends Mongo{
        
        /***
         *  Default Database that this connection will reference
         ***/
        public $db; //open all the default functions to the public as well
        public $debug=false; //debug mode (not fully implemented in all functions yet)
        public $default_upsert_state=false; //default upsert state
        public $dbName;
        
        /***
         *  Opens a connection to the desired DB and stores the connection in the object
         *
         *  @param  $name   name of the database to use
         ***/
        public function setDBName($name){
            $this->db = $this->selectDB($name);
            $this->dbName = $name; //this is cached just for ExecuteAdminCommand to work fine
        }
        
        
        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $column     String  Name of the Column to go for
         *  @param  $filter     Array   Filter settings to use
         *  @param  $order      Array   Sort preferences
         *  @param  $slaveOkay  Bool    If a read from the slave is allowed
         *
         *  @return mixed       
         ***/
        public function getOne($collection, $column, Array $filter=array(), Array $order=array(), $slaveOkay=true){
            //get all burning items from the queue
            $cursor = $this->db->$collection->find($filter);  //SELECT * FROM $collection WHERE ^^^            
            
            if(!$slaveOkay){
                $cursor->slaveOkay(false);
            }
            
            if($order){
                $cursor->sort($order);  //ORDER BY $order ASC
            }
            $cursor->limit(1);          //LIMIT 1  

            $this->debug('<span style="color:#0A0;">getOne</span> from '.$collection.', column '.$column.', filter:<br />'.print_r($filter, true).'Order By:<br />'.print_r($order,true));
            
            foreach($cursor as $obj){                
                //close cursor before return
                unset($cursor);                
                
                return $obj[$column];
            }
            
            return false;
        }
        
        
        /***
         *  Find one Document Key-Val by MongoID
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $id         String  MongoID of the Document to Delete
         *
         *  @return mixed
         ***/
        public function getOneByID($collection, $column, $id){
            return $this->getOne($collection, $column, array('_id' => new MongoId($id)));
        }
        
        
        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter     Array   Filter settings to use
         *  @param  $order      Array   Sort preferences
         *  @param  $start      Int     LIMIT _X_, Y (if set) 
         *  @param  $offset     Int     LIMIT X, _Y_ (if set)
         *
         *  @return array of arrays
         ***/
        public function getAll($collection, Array $filter=array(), Array $order=array(), $start=null, $offset=null, $slaveOkay=true){
            $cursor = $this->db->$collection->find($filter)->limit((int)$start)->skip((int)$offset);  //SELECT * FROM $collection WHERE ^^^            
            
            if(!$slaveOkay){
                $cursor->slaveOkay(false);
            }
            
            if($order){
                $cursor->sort($order);  //ORDER BY $order ASC
            }           

            //limit x,y            
            /*
            if($start){
                $cursor->limit($start);
            }
            
            if($offset){
                $cursor->skip((int)$offset);
            }
            */
            
            $return = array();
            foreach($cursor as $obj){
                $return[] = $obj;
            }
            
            //close cursor before return
            unset($cursor);
                
            $this->debug('<span style="color:#0A0;">getAll</span> from '.$collection.' filter:<br />'.print_r($filter, true).'Order:<br/>'.print_r($order,true).'LIMIT: '.$start.' OFFSET: '.$offset);            
            return $return;
        }
        
        
        /***
         *  @param  $collection Array   Name of the Collection (Table) to query
         *  @param  $column     String  Reduce the resultset to THIS column
         *  @param  $filter     Array   Filter settings to use
         *  @param  $order      Array   Sort preferences
         *  @param  $start      Int     LIMIT _X_, Y (if set) 
         *  @param  $offset     Int     LIMIT X, _Y_ (if set)
         *
         *  @return array representing a columnt from a collection
         ***/
        public function getColumn($collection, $column, Array $filter=array(), Array $order=array(), $start=null, $offset=null){
            $result = $this->getAll($collection, $filter, $order, $start, $offset);
            $return = array();
            foreach($result as $obj){
                $return[] = $obj[$column];
            }
            
            return $return;
        }
        
        
        /***
         *  Shorthand alias for the function above getColumn()
         *  @param  $collection Array   Name of the Collection (Table) to query
         *  @param  $column     String  Reduce the resultset to THIS column
         *  @param  $filter     Array   Filter settings to use
         *  @param  $order      Array   Sort preferences
         *  @param  $start      Int     LIMIT _X_, Y (if set) 
         *  @param  $offset     Int     LIMIT X, _Y_ (if set)
         *
         *  @return array representing a columnt from a collection
         ***/
        public function getCol($collection, $column, Array $filter=array(), Array $order=array(), $start=null, $offset=null){
            return $this->getColumn($collection, $column, $filter, $order, $start, $offset);
        }
        
        
        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter     Array   Filter settings to use
         *  @param  $order      Array   Sort preferences
         *
         *  @return array representing one document (however complex it may be)
         ***/
        public function getRow($collection, Array $filter=array(), Array $order=array(), $slaveOkay=true){
            $result = $this->getAll($collection, $filter, $order, 1, null, $slaveOkay);
            foreach($result as $obj){
                return $obj;
            }
            return array();
        }
        
        
        /***
         *  Find one Document by MongoID
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $id         String  MongoID of the Document to Delete
         *
         *  @return array representing one document (however complex it may be)
         ***/
        public function getRowByID($collection, $id, $slaveOkay=true){
            return $this->getRow($collection, array('_id' => new MongoId($id)), array(), $slaveOkay);
        }
        
        
        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter     Array   Filter settings to use
         *  @param  $order      Array   Sort preferences
         *  @param  $start      Int     LIMIT _X_, Y (if set) 
         *  @param  $offset     Int     LIMIT X, _Y_ (if set)
         *
         *  @return array of arrays with the keys of the first array taken from a filter
         ***/
        public function getAssoc($collection, $key, $val=null, Array $filter=array(), Array $order=array(), $start=null, $offset=null){
            $result = $this->getAll($collection, $filter, $order, $start, $offset);
            $return = array();
            if($val){ //it eez faster to have the IF outside of the loop
                foreach($result as $obj){
                    $return[$obj[$key]] = $obj[$val];
                }
            }else{
                foreach($result as $obj){
                    $return[$obj[$key]] = $obj;
                }
            }

            //adding debug info where neccessary            
            $this->debug('<span style="color:#0A0;">getAssoc</span> from '.$collection.' with filter:<br />'.print_r($filter,true).'<br />key is: '.$key.'<br />Order: '.print_r($order,true).'<br />Start: '.$start.'<br />Offset:'.$offset);            
            return $return;
        }
        
        
        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $document   Array   The document to store in the collection
         *
         *  @return void
         ***/
        public function insert($collection, Array $document){
            $this->db->$collection->insert($document);            
            $this->debug('<span style="color:#0A0;">Inserted</span> Document into '.$collection.':<br />'.print_r($document, true));
        }
        
        
        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter
         *  @param  $data
         *  @param  $options
         ***/
        public function update($collection, Array $filter, Array $data, Array $options=null){
            if(!isset($options['multiple'])){
                $options['multiple'] = true; //enabled per default
            }
            
            if(!isset($options['upsert'])){
                $options['upsert']   = $this->default_upsert_state; //disabled per default!!!
            }
            
            //cook data for the update
            $data = array('$set' => $data);
            
            //did I pass an incrementation string with the options?
            if(isset($options['$inc'])){
                $data['$inc'] = $options['$inc'];
                unset($options['$inc']);
            }
            
            //debug message
            $this->debug('<span style="color:#AA0;">Updated</span> Document in '.$collection.':<br />'.print_r($data, true).'with filter:<br />'.print_r($filter, true).'Using these options:<br />'.print_r($options, true));
            
            return $this->db->$collection->update($filter, $data, $options);
        }
        

        /***
         *  Same as update with will alter maximum of one record
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter
         *  @param  $data
         *  @param  $options
         ***/
        public function updateOne($collection, Array $filter, Array $data, Array $options=null){
            //update one record only
            $options['multiple'] = false;
            return $this->update($collection, $filter, $data, $options);
        }
        
        
        /***
         *  Same as update, updates only one records found by ID
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $id
         *  @param  $data
         *  @param  $options
         ***/
        public function updateOneByID($collection, $id, Array $data, Array $options=null){
            //update one record only
            $options['multiple'] = false;

			if (!($id instanceof MongoId)) {
				$id = new MongoId($id);
			}

            return $this->update($collection, array('_id' => $id), $data, $options);
        }
        
        
        /***
         *  Shorthand alias for the function above
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $id
         *  @param  $data
         *  @param  $options
         ***/
        public function updateByID($collection, $id, Array $data, Array $options=null){
            return $this->updateOneByID($collection, $id, $data, $options);
        }


        /***
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter     Array   Filter settings to use
         *
         *  @return boolean
         ***/
        public function delete($collection, Array $filter=array()){
            $this->debug('<span style="color:#F00;">Delete</span> from '.$collection.' filter:<br />'.print_r($filter, true));            
            return $this->db->$collection->remove($filter);
        }
        
        
        /***
         *  Remove a document from Collection based on its MongoDB
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $id         String  MongoID of the Document to Delete
         *
         *  @return boolean
         ***/
        public function deleteByID($collection, $id){
			if (!($id instanceof MongoId)) {
				$id = new MongoId($id);
			}

            return $this->delete($collection, array('_id' => $id), true);
        }
        
        
        /***
         *  Count the number of matching documents
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $filter     Array   Filter settings to use
         *  @param  $start      Int     LIMIT _X_, Y (if set) 
         *  @param  $offset     Int     LIMIT X, _Y_ (if set)
         *
         *  @return integer
         ***/
        public function count($collection, Array $filter=array(), $start=null, $offset=null){
            if(count($filter)>0){
                return $this->db->$collection->count($filter, $start, $offset);
            }else{
                return $this->db->$collection->count();
            }            
        }
        
        
        /***
         *  Aggregation in MongoDB is fun
         *
         *  @param  $collection String
         *  @param  $keys
         *  @param  $initial
         *  @param  $reduce
         ***/
        public function group($collection, Array $keys, Array $initial, $reduce, Array $filter=array()){
            return $this->db->$collection->group($keys, $initial, $reduce, $filter);
        }
        
        
        public function groupAssoc($collection, Array $keys, Array $initial, $reduce, Array $filter=array()){
            $data = $this->group($collection, $keys, $initial, $reduce, $filter);
            $key  = array_shift(array_keys($keys));
            $val  = array_shift(array_keys($initial));
            
            $return = array();
            foreach($data['retval'] as $item){
                $return[$item[$key]] = $item[$val];
            }
            return $return;
        }

        
        /***
         *  Executes some other Mongo Command vie the "command" method internal to mongodb
         *  this method is named "Execute" just to resemble the common "Execute" method
         *  in AdoDB, it is, however, not a pipe to mongo db's execute method
         *
         *  @param  $command    Array   Full Mongo command to Execute
         *
         *  @return mixed the command return values
         ***/        
        public function Execute(Array $command){
            $this->debug('Executing Command: '.print_r($command, true));           
            
            $cursor = $this->db->command($command);
            $return = array();
            foreach($cursor as $obj){
                $return[] = $obj;
            }      
            
            //close cursor before return
            unset($cursor);      
            
            $this->debug('Admin Command Result: '.print_r($return, true));
            
            return $return;
        }
        
        
        /***
         *  Executes raw Javascript code (whatever you want)
         ***/
        public function ExecuteJavaScript($code, $arguments=array(), $nolock=true){
            //$code = new MongoCode($code);
            $this->debug('Javascript Command Execution: <pre>'.print_r($code,true).'</pre>');
            $result = $this->db->command(array('$eval'=>$code, 'args'=>$arguments), array('nolock'=>$nolock));
            $this->debug('Result: '.print_r($result,true));
            return $result;
        }
        
        
        /***
         *  Executes a command agains the Admin Database (useful for RS operations)
         *  
         *  @param  $command    Array   Full Mongo command to Execute
         *
         *  @return mixed the command return values
         ***/
        public function ExecuteAdminCommand(Array $command){
            $this->setDBName('admin');
            $return = $this->Execute($command);
            $this->setDBName($this->dbName); //back to the original DB
            return $return;
        }
        
        
        /***
         *  Convenience method for creating an index
         *
         *  @param  $collection String  Name of the Collection (Table) to query
         *  @param  $settings   Array   Indexing Settings
         ***/
        public function ensureIndex($collection, Array $settings, Array $options=array()){
            return $this->db->$collection->ensureIndex($settings, $options);
        }
        
        
        /***
         *  Used for outputting debug info to browser
         ***/
        public function debug($message){
            if($this->debug){
                echo '<blockquote style="padding: 10px; margin: 5px; background: #eee; border: 1px solid #ccc;">
                      <pre>'.date('Y-m-d H:i:s').' '.trim($message).'</pre>
                      </blockquote>';
            }
        }
    }