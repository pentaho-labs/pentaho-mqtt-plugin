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

package org.pentaho.di.trans.steps.pentahomqttpublisher;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MQTTPublisherMetaTest {

  @BeforeClass public static void beforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  @SuppressWarnings( "unchecked" )
  @Test public void testRoundTrips() throws KettleException, NoSuchMethodException, SecurityException {
    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "CA_FILE", "getSSLCaFile" );
    getterMap.put( "CERT_FILE", "getSSLCertFile" );
    getterMap.put( "KEY_FILE", "getSSLKeyFile" );
    getterMap.put( "KEY_FILE_PASS", "getSSLKeyFilePass" );
    getterMap.put( "KEEP_ALIVE", "getKeepAliveInterval" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "CA_FILE", "setSSLCaFile" );
    setterMap.put( "CERT_FILE", "setSSLCertFile" );
    setterMap.put( "KEY_FILE", "setSSLKeyFile" );
    setterMap.put( "KEY_FILE_PASS", "setSSLKeyFilePass" );
    setterMap.put( "KEEP_ALIVE", "setKeepAliveInterval" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
    Map<String, FieldLoadSaveValidator<?>>
        fieldLoadSaveValidatorTypeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();

    LoadSaveTester
        tester =
        new LoadSaveTester( MQTTPublisherMeta.class,
            Arrays.<String>asList( "broker", "topic", "topicIsFromField", "field", "client_id", "timeout", "qo_s",
                "requires_auth", "password", "username" ), getterMap, setterMap,
            fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    tester.testSerialization();
  }
}
