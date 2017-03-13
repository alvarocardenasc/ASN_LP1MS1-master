/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.iteso.desi.cloud.hw1;

import com.amazonaws.regions.Regions;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;

/**
 *
 * @author parres
 */
public class Config {

  // The type of key/value store you are using. Initially set to BERKELEY;
  // will be changed to DynamoDB in some phases.
  //public static final KeyValueStoreFactory.STORETYPE storeType = KeyValueStoreFactory.STORETYPE.BERKELEY;
  public static final KeyValueStoreFactory.STORETYPE storeType = KeyValueStoreFactory.STORETYPE.DYNAMODB;
  

//public static final String pathToDatabase = "c:/dbd";
  public static final String pathToDatabase = "/home/asn2/Desktop/practica1asn/berkdb/";
    
  // Set to your Amazon Access Key ID
  // NEVER SHARE THIS INFORMATION. SO PLEASE SET IT TO "" WHEN YOU UPLOAD YOUR HOMEWORK 
  public static final String accessKeyID = "";
  
  // Set to your Amazon Secret Access Key
  // NEVER SHARE THIS INFORMATION. SO PLEASE SET IT TO "" WHEN YOU UPLOAD YOUR HOMEWORK 
  public static final String secretAccessKey = "";

  // Set to your Amazon REGION tu be used
  public static final Regions amazonRegion = Regions.US_EAST_1;
  
    
  // Restrict the topics that should be indexed. For example, when this is
  // set to 'X', you should only index topics that start with an X.
  // Set this to "A" for local work, and to "Ar" for cloud tests..
  public static final String filter = "Ar";
  
  public static final String titleFileName = "labels_en.ttl";
  public static final String imageFileName = "images_en.ttl";
  
  public static final String pathToFiles = "/home/asn2/NetBeansProjects/ASN_LP1MS1-master/";
    
}
