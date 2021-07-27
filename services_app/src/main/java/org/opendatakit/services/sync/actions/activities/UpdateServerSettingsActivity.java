package org.opendatakit.services.sync.actions.activities;

import androidx.appcompat.app.AppCompatActivity;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.Toast;

import com.google.android.material.textfield.TextInputLayout;
import com.google.zxing.integration.android.IntentIntegrator;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.utilities.ODKFileUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static androidx.core.content.ContextCompat.checkSelfPermission;

public class UpdateServerSettingsActivity extends AppCompatActivity {

    private class OnButtonClick implements View.OnClickListener{
        @Override
        public void onClick(View v) {
            if(v.getId()==R.id.btnUpdateServerUrl){
                String url=inputServerUrl.getEditText().getText().toString();
                if(url.isEmpty()){
                    inputServerUrl.setError("Server URL cannot be empty!");
                    return;
                }
                if(!url.startsWith("https://")){
                    url="https://"+url;
                }
                if(!isValidUrl(url)){
                    inputServerUrl.setError("Please enter a Valid URL");
                }
                else{
                    inputServerUrl.setError("");
                    updateServerUrl(url);
                }
            }
            else if(v.getId()==R.id.btnChooseDefaultServer){
                inputServerUrl.getEditText().setText(getString(R.string.default_sync_server_url));
            }
            else if(v.getId()==R.id.btnVerifyServerUpdateServerDetails){
                Intent intent=new Intent(UpdateServerSettingsActivity.this,VerifyServerSettingsActivity.class);
                intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME,getAppName());
                startActivity(intent);
            }
            else if(v.getId()==R.id.btnScanQrUpdateServerDetails){

            }
        }
    }

    private String mAppName;

    private TextInputLayout inputServerUrl;

    private PropertiesSingleton props;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.update_server_settings);
        findViewsAndAttachListeners();
    }

    @Override
    protected void onResume() {
        super.onResume();
        mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
        if (mAppName == null) {
            mAppName = ODKFileUtils.getOdkDefaultAppName();
        }
        updateInterface();
    }

    private void findViewsAndAttachListeners(){
        inputServerUrl=findViewById(R.id.inputServerUrl);
        Button btnUpdateUrl = findViewById(R.id.btnUpdateServerUrl);
        Button btnSetDefault = findViewById(R.id.btnChooseDefaultServer);
        Button btnVerifyServerDetails = findViewById(R.id.btnVerifyServerUpdateServerDetails);
        Button btnScanQr = findViewById(R.id.btnScanQrUpdateServerDetails);

        OnButtonClick onButtonClick=new OnButtonClick();
        btnUpdateUrl.setOnClickListener(onButtonClick);
        btnSetDefault.setOnClickListener(onButtonClick);
        btnVerifyServerDetails.setOnClickListener(onButtonClick);
        btnScanQr.setOnClickListener(onButtonClick);
    }

    private void updateInterface(){
        props= CommonToolProperties.get(this,getAppName());
        String serverUrl=props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);

        inputServerUrl.getEditText().setText(serverUrl);
    }

    private void updateServerUrl(String url){

        String currUrl=props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
        if(currUrl.equals(url)){
            Toast.makeText(this, "No Change in the Server URL", Toast.LENGTH_SHORT).show();
            return;
        }

        Map<String,String> properties = new HashMap<>();
        properties.put(CommonToolProperties.KEY_SYNC_SERVER_URL,url);
        properties.put(CommonToolProperties.KEY_USERNAME, "");
        properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(false));
        properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.LOGGED_OUT.name());
        properties.remove(CommonToolProperties.KEY_LAST_SYNC_INFO);
        properties.put(CommonToolProperties.KEY_IS_SERVER_VERIFIED, Boolean.toString(false));
        properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(false));
        properties.remove(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED);
        properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
        properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
        properties.put(CommonToolProperties.KEY_USERS_LIST, "");
        props.setProperties(properties);
        updateInterface();
        Toast.makeText(this, "Server URL Updated Successfully!", Toast.LENGTH_SHORT).show();
    }

    private boolean isValidUrl(String url){
        if(url.contains(" ") || !url.equals(url.trim())){
            return false;
        }

        while (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        boolean isValid = false;
        try {
            new URL(url);
            isValid = true;
        } catch (MalformedURLException e) {
            // ignore
        }

        return isValid;
    }

    public String getAppName() {
        return mAppName;
    }
}