/*******************************************************************************
 *
 * Pentaho IoT
 *
 * Copyright (C) 2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.pentahomqttsubscriber;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta class for the MQTTSubscriber step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
@Step( id = "MQTTSubscriberMeta", image = "MQTTSubscriberIcon.svg", name = "MQTT Subscriber", description =
    "Subscribe to topics " + "at an MQTT broker", categoryDescription = "Input" ) public class MQTTSubscriberMeta
    extends BaseStepMeta implements StepMetaInterface {

  protected String m_broker = "";

  protected List<String> m_topics = new ArrayList<>();

  protected String m_messageType = ValueMetaFactory.getValueMetaName( ValueMetaInterface.TYPE_STRING );

  private String m_clientId;
  private String m_timeout = "30"; // seconds according to the mqtt javadocs
  private String m_keepAliveInterval = "60"; // seconds according to the mqtt javadocs
  private String m_qos = "0";
  private boolean m_requiresAuth;
  private String m_username;
  private String m_password;
  private String m_sslCaFile;
  private String m_sslCertFile;
  private String m_sslKeyFile;
  private String m_sslKeyFilePass;

  /**
   * Whether to allow messages of type object to be deserialized off the wire
   */
  private boolean m_allowReadObjectMessageType;

  /**
   * Execute for x seconds (0 means indefinitely)
   */
  private String m_executeForDuration = "0";

  /**
   * @return Broker URL
   */
  public String getBroker() {
    return m_broker;
  }

  /**
   * @param broker Broker URL
   */
  public void setBroker( String broker ) {
    m_broker = broker;
  }

  public void setTopics( List<String> topics ) {
    m_topics = topics;
  }

  public List<String> getTopics() {
    return m_topics;
  }

  /**
   * @param type the Kettle type of the message being received
   */
  public void setMessageType( String type ) {
    m_messageType = type;
  }

  /**
   * @return the Kettle type of the message being received
   */
  public String getMessageType() {
    return m_messageType;
  }

  /**
   * @return Client ID
   */
  public String getClientId() {
    return m_clientId;
  }

  /**
   * @param clientId Client ID
   */
  public void setClientId( String clientId ) {
    m_clientId = clientId;
  }

  /**
   * @return Connection m_timeout
   */
  public String getTimeout() {
    return m_timeout;
  }

  /**
   * @param timeout Connection m_timeout
   */
  public void setTimeout( String timeout ) {
    m_timeout = timeout;
  }

  /**
   * @param interval interval in seconds
   */
  public void setKeepAliveInterval( String interval ) {
    m_keepAliveInterval = interval;
  }

  /**
   * @return the keep alive interval (in seconds)
   */
  public String getKeepAliveInterval() {
    return m_keepAliveInterval;
  }

  /**
   * @return QoS to use
   */
  public String getQoS() {
    return m_qos;
  }

  /**
   * @param qos QoS to use
   */
  public void setQoS( String qos ) {
    m_qos = qos;
  }

  /**
   * @return Whether MQTT broker requires authentication
   */
  public boolean isRequiresAuth() {
    return m_requiresAuth;
  }

  /**
   * @param requiresAuth Whether MQTT broker requires authentication
   */
  public void setRequiresAuth( boolean requiresAuth ) {
    m_requiresAuth = requiresAuth;
  }

  /**
   * @return Username to MQTT broker
   */
  public String getUsername() {
    return m_username;
  }

  /**
   * @param username Username to MQTT broker
   */
  public void setUsername( String username ) {
    m_username = username;
  }

  /**
   * @return Password to MQTT broker
   */
  public String getPassword() {
    return m_password;
  }

  /**
   * @param password Password to MQTT broker
   */
  public void setPassword( String password ) {
    m_password = password;
  }

  /**
   * @return Server CA file
   */
  public String getSSLCaFile() {
    return m_sslCaFile;
  }

  /**
   * @param sslCaFile Server CA file
   */
  public void setSSLCaFile( String sslCaFile ) {
    m_sslCaFile = sslCaFile;
  }

  /**
   * @return Client certificate file
   */
  public String getSSLCertFile() {
    return m_sslCertFile;
  }

  /**
   * @param sslCertFile Client certificate file
   */
  public void setSSLCertFile( String sslCertFile ) {
    m_sslCertFile = sslCertFile;
  }

  /**
   * @return Client key file
   */
  public String getSSLKeyFile() {
    return m_sslKeyFile;
  }

  /**
   * @param sslKeyFile Client key file
   */
  public void setSSLKeyFile( String sslKeyFile ) {
    m_sslKeyFile = sslKeyFile;
  }

  /**
   * @return Client key file m_password
   */
  public String getSSLKeyFilePass() {
    return m_sslKeyFilePass;
  }

  /**
   * @param sslKeyFilePass Client key file m_password
   */
  public void setSSLKeyFilePass( String sslKeyFilePass ) {
    m_sslKeyFilePass = sslKeyFilePass;
  }

  /**
   * @param duration the duration (in seconds) to run for. 0 indicates run indefinitely
   */
  public void setExecuteForDuration( String duration ) {
    m_executeForDuration = duration;
  }

  /**
   * @return the number of seconds to run for (0 means run indefinitely)
   */
  public String getExecuteForDuration() {
    return m_executeForDuration;
  }

  /**
   * @param allow true to allow object messages to be deserialized off of the wire
   */
  public void setAllowReadMessageOfTypeObject( boolean allow ) {
    m_allowReadObjectMessageType = allow;
  }

  /**
   * @return true if deserializing object messages is ok
   */
  public boolean getAllowReadMessageOfTypeObject() {
    return m_allowReadObjectMessageType;
  }

  @Override public void setDefault() {

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta,
      Trans trans ) {
    return new MQTTSubscriber( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new MQTTSubscriberData();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {
    m_broker = XMLHandler.getTagValue( stepnode, "BROKER" );
    String topics = XMLHandler.getTagValue( stepnode, "TOPICS" );
    m_topics = new ArrayList<>();
    if ( !Const.isEmpty( topics ) ) {
      String[] parts = topics.split( "," );
      for ( String p : parts ) {
        m_topics.add( p.trim() );
      }
    }

    m_messageType = XMLHandler.getTagValue( stepnode, "MESSAGE_TYPE" );
    if ( Const.isEmpty( m_messageType ) ) {
      m_messageType = ValueMetaFactory.getValueMetaName( ValueMetaInterface.TYPE_STRING );
    }
    m_clientId = XMLHandler.getTagValue( stepnode, "CLIENT_ID" );
    m_timeout = XMLHandler.getTagValue( stepnode, "TIMEOUT" );
    m_keepAliveInterval = XMLHandler.getTagValue( stepnode, "KEEP_ALIVE" );
    m_executeForDuration = XMLHandler.getTagValue( stepnode, "EXECUTE_FOR_DURATION" );
    m_qos = XMLHandler.getTagValue( stepnode, "QOS" );
    m_requiresAuth = Boolean.parseBoolean( XMLHandler.getTagValue( stepnode, "REQUIRES_AUTH" ) );

    m_username = XMLHandler.getTagValue( stepnode, "USERNAME" );
    m_password = XMLHandler.getTagValue( stepnode, "PASSWORD" );
    if ( !Const.isEmpty( m_password ) ) {
      m_password = Encr.decryptPasswordOptionallyEncrypted( m_password );
    }

    String allowObjects = XMLHandler.getTagValue( stepnode, "READ_OBJECTS" );
    if ( !Const.isEmpty( allowObjects ) ) {
      m_allowReadObjectMessageType = Boolean.parseBoolean( allowObjects );
    }

    Node sslNode = XMLHandler.getSubNode( stepnode, "SSL" );
    if ( sslNode != null ) {
      m_sslCaFile = XMLHandler.getTagValue( sslNode, "CA_FILE" );
      m_sslCertFile = XMLHandler.getTagValue( sslNode, "CERT_FILE" );
      m_sslKeyFile = XMLHandler.getTagValue( sslNode, "KEY_FILE" );
      m_sslKeyFilePass = XMLHandler.getTagValue( sslNode, "KEY_FILE_PASS" );
    }
  }

  @Override public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    if ( !Const.isEmpty( m_broker ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "BROKER", m_broker ) );
    }

    if ( !Const.isEmpty( m_topics ) ) {
      String topicString = "";
      for ( String t : m_topics ) {
        topicString += "," + t;
      }
      retval.append( "    " ).append( XMLHandler.addTagValue( "TOPICS", topicString.substring( 1 ) ) );
    }

    if ( !Const.isEmpty( m_messageType ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "MESSAGE_TYPE", m_messageType ) );
    }

    if ( !Const.isEmpty( m_clientId ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "CLIENT_ID", m_clientId ) );
    }
    if ( !Const.isEmpty( m_timeout ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "TIMEOUT", m_timeout ) );
    }
    if ( !Const.isEmpty( m_qos ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "QOS", m_qos ) );
    }
    if ( !Const.isEmpty( m_keepAliveInterval ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "KEEP_ALIVE", m_keepAliveInterval ) );
    }
    if ( !Const.isEmpty( m_executeForDuration ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "EXECUTE_FOR_DURATION", m_executeForDuration ) );
    }
    retval.append( "    " ).append( XMLHandler.addTagValue( "REQUIRES_AUTH", Boolean.toString( m_requiresAuth ) ) );
    if ( !Const.isEmpty( m_username ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "USERNAME", m_username ) );
    }
    if ( !Const.isEmpty( m_password ) ) {
      retval.append( "    " )
          .append( XMLHandler.addTagValue( "PASSWORD", Encr.encryptPasswordIfNotUsingVariables( m_password ) ) );
    }

    retval.append( "    " )
        .append( XMLHandler.addTagValue( "READ_OBJECTS", Boolean.toString( m_allowReadObjectMessageType ) ) );

    if ( !Const.isEmpty( m_sslCaFile ) || !Const.isEmpty( m_sslCertFile ) || !Const.isEmpty( m_sslKeyFile ) || !Const
        .isEmpty( m_sslKeyFilePass ) ) {
      retval.append( "    " ).append( XMLHandler.openTag( "SSL" ) ).append( Const.CR );
      if ( !Const.isEmpty( m_sslCaFile ) ) {
        retval.append( "      " + XMLHandler.addTagValue( "CA_FILE", m_sslCaFile ) );
      }
      if ( !Const.isEmpty( m_sslCertFile ) ) {
        retval.append( "      " + XMLHandler.addTagValue( "CERT_FILE", m_sslCertFile ) );
      }
      if ( !Const.isEmpty( m_sslKeyFile ) ) {
        retval.append( "      " + XMLHandler.addTagValue( "KEY_FILE", m_sslKeyFile ) );
      }
      if ( !Const.isEmpty( m_sslKeyFilePass ) ) {
        retval.append( "      " + XMLHandler.addTagValue( "KEY_FILE_PASS", m_sslKeyFilePass ) );
      }
      retval.append( "    " ).append( XMLHandler.closeTag( "SSL" ) ).append( Const.CR );
    }

    return retval.toString();
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases )
      throws KettleException {
    m_broker = rep.getStepAttributeString( stepId, "BROKER" );
    String topics = rep.getStepAttributeString( stepId, "TOPICS" );
    m_topics = new ArrayList<>();
    if ( !Const.isEmpty( topics ) ) {
      String[] parts = topics.split( "," );
      for ( String p : parts ) {
        m_topics.add( p.trim() );
      }
    }
    m_messageType = rep.getStepAttributeString( stepId, "MESSAGE_TYPE" );
    if ( Const.isEmpty( m_messageType ) ) {
      m_messageType = ValueMetaFactory.getValueMetaName( ValueMetaInterface.TYPE_STRING );
    }
    m_clientId = rep.getStepAttributeString( stepId, "CLIENT_ID" );
    m_timeout = rep.getStepAttributeString( stepId, "TIMEOUT" );
    m_keepAliveInterval = rep.getStepAttributeString( stepId, "KEEP_ALIVE" );
    m_executeForDuration = rep.getStepAttributeString( stepId, "EXECUTE_FOR_DURATION" );
    m_qos = rep.getStepAttributeString( stepId, "QOS" );
    m_requiresAuth = Boolean.parseBoolean( rep.getStepAttributeString( stepId, "REQUIRES_AUTH" ) );
    m_username = rep.getStepAttributeString( stepId, "USERNAME" );
    m_password = rep.getStepAttributeString( stepId, "PASSWORD" );
    m_allowReadObjectMessageType = Boolean.parseBoolean( rep.getStepAttributeString( stepId, "READ_OBJECTS" ) );

    m_sslCaFile = rep.getStepAttributeString( stepId, "SSL_CA_FILE" );
    m_sslCertFile = rep.getStepAttributeString( stepId, "SSL_CERT_FILE" );
    m_sslKeyFile = rep.getStepAttributeString( stepId, "SSL_KEY_FILE" );
    m_sslKeyFilePass = rep.getStepAttributeString( stepId, "SSL_KEY_FILE_PASS" );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
      throws KettleException {
    if ( !Const.isEmpty( m_broker ) ) {
      rep.saveStepAttribute( transformationId, stepId, "BROKER", m_broker );
    }
    if ( !Const.isEmpty( m_topics ) ) {
      String topicString = "";
      for ( String t : m_topics ) {
        topicString += "," + t;
      }
      rep.saveStepAttribute( transformationId, stepId, "TOPICS", topicString.substring( 1 ) );
    }
    if ( !Const.isEmpty( m_messageType ) ) {
      rep.saveStepAttribute( transformationId, stepId, "MESSAGE_TYPE", m_messageType );
    }
    if ( !Const.isEmpty( m_clientId ) ) {
      rep.saveStepAttribute( transformationId, stepId, "CLIENT_ID", m_clientId );
    }
    if ( !Const.isEmpty( m_timeout ) ) {
      rep.saveStepAttribute( transformationId, stepId, "TIMEOUT", m_timeout );
    }
    if ( !Const.isEmpty( m_keepAliveInterval ) ) {
      rep.saveStepAttribute( transformationId, stepId, "KEEP_ALIVE", m_keepAliveInterval );
    }
    if ( !Const.isEmpty( m_executeForDuration ) ) {
      rep.saveStepAttribute( transformationId, stepId, "EXECUTE_FOR_DURATION", m_executeForDuration );
    }
    if ( !Const.isEmpty( m_qos ) ) {
      rep.saveStepAttribute( transformationId, stepId, "QOS", m_qos );
    }
    rep.saveStepAttribute( transformationId, stepId, "REQUIRES_AUTH", Boolean.toString( m_requiresAuth ) );
    if ( !Const.isEmpty( m_username ) ) {
      rep.saveStepAttribute( transformationId, stepId, "USERNAME", m_username );
    }
    if ( !Const.isEmpty( m_password ) ) {
      rep.saveStepAttribute( transformationId, stepId, "PASSWORD", m_password );
    }

    rep.saveStepAttribute( transformationId, stepId, "READ_OBJECTS", Boolean.toString( m_allowReadObjectMessageType ) );

    if ( !Const.isEmpty( m_sslCaFile ) ) {
      rep.saveStepAttribute( transformationId, stepId, "SSL_CA_FILE", m_sslCaFile );
    }
    if ( !Const.isEmpty( m_sslCertFile ) ) {
      rep.saveStepAttribute( transformationId, stepId, "SSL_CERT_FILE", m_sslCertFile );
    }
    if ( !Const.isEmpty( m_sslKeyFile ) ) {
      rep.saveStepAttribute( transformationId, stepId, "SSL_KEY_FILE", m_sslKeyFile );
    }
    if ( !Const.isEmpty( m_sslKeyFilePass ) ) {
      rep.saveStepAttribute( transformationId, stepId, "SSL_KEY_FILE_PASS", m_sslKeyFilePass );
    }

  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String stepName, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repo, IMetaStore metaStore ) throws KettleStepException {

    rowMeta.clear();
    try {
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Topic", ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta(
          ValueMetaFactory.createValueMeta( "Message", ValueMetaFactory.getIdForValueMeta( getMessageType() ) ) );
    } catch ( KettlePluginException e ) {
      throw new KettleStepException( e );
    }
  }
}
