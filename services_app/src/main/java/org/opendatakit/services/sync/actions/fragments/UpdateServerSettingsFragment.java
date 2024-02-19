package org.opendatakit.services.sync.actions.fragments;

import static androidx.core.content.ContextCompat.checkSelfPermission;

import android.Manifest;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

import androidx.activity.OnBackPressedCallback;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.lifecycle.ViewModelProvider;
import androidx.navigation.NavController;
import androidx.navigation.Navigation;

import com.google.android.material.appbar.MaterialToolbar;
import com.google.android.material.dialog.MaterialAlertDialogBuilder;
import com.google.android.material.textfield.TextInputLayout;
import com.google.zxing.integration.android.IntentIntegrator;
import com.google.zxing.integration.android.IntentResult;

import org.json.JSONException;
import org.json.JSONObject;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.AbsSyncBaseActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.UserState;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UpdateServerSettingsFragment extends Fragment {

    private class OnButtonClick implements View.OnClickListener {
        @Override
        public void onClick(View v) {
            if (v.getId() == R.id.btnUpdateServerUrl) {
                onClickUpdateUrl();
            } else if (v.getId() == R.id.btnChooseDefaultServer) {
                inputServerUrl.getEditText().setText(getString(R.string.default_sync_server_url));
            } else if (v.getId() == R.id.btnVerifyServerUpdateServerDetails) {
                startVerifyActivity();
            } else if (v.getId() == R.id.btnScanQrUpdateServerDetails) {
                startQrCodeScan();
            }
        }
    }

    private final int PERMISSION_REQUEST_CAMERA_CODE = 1;
    protected static final String[] CAMERA_PERMISSION = new String[] {
            Manifest.permission.CAMERA
    };

    private TextInputLayout inputServerUrl;
    private AbsSyncViewModel absSyncViewModel;
    private NavController navController;
    private Button btnUpdateUrl, btnSetDefault, btnVerifyServerDetails, btnScanQr;

    MaterialToolbar materialToolbar;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_update_server_settings, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);

        materialToolbar=view.findViewById(R.id.topAppBar1);
        materialToolbar.setNavigationOnClickListener(v -> {

            //Using nav controller to navigate back to previous stack
            //Checkout res >> navigation >> nav_graph_main.xml to understand the navigation graph better
            Navigation.findNavController(view).popBackStack();

        });
        findViewsAndAttachListeners(view);
        setupViewModelAndNavController();

    }

    private void findViewsAndAttachListeners(View view) {
        inputServerUrl = view.findViewById(R.id.inputServerUrl);
        btnUpdateUrl = view.findViewById(R.id.btnUpdateServerUrl);
        btnSetDefault = view.findViewById(R.id.btnChooseDefaultServer);
        btnVerifyServerDetails = view.findViewById(R.id.btnVerifyServerUpdateServerDetails);
        btnScanQr = view.findViewById(R.id.btnScanQrUpdateServerDetails);

        OnButtonClick onButtonClick = new OnButtonClick();
        btnUpdateUrl.setOnClickListener(onButtonClick);
        btnSetDefault.setOnClickListener(onButtonClick);
        btnVerifyServerDetails.setOnClickListener(onButtonClick);
        btnScanQr.setOnClickListener(onButtonClick);
    }

    private void setupViewModelAndNavController() {
        if(requireActivity().getClass().getName().equals(MainActivity.class.getName()))
            absSyncViewModel = new ViewModelProvider(requireActivity()).get(AbsSyncViewModel.class);
        else
            absSyncViewModel = ((AbsSyncBaseActivity)requireActivity()).getViewModel();
        navController = Navigation.findNavController(requireView());
        absSyncViewModel.getServerUrl().observe(getViewLifecycleOwner(), s -> inputServerUrl.getEditText().setText(s));
    }

    private void startVerifyActivity() {
        Intent intent = new Intent(requireActivity(), VerifyServerSettingsActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, absSyncViewModel.getAppName());
        startActivity(intent);
    }

    private void onClickUpdateUrl() {
        String url = inputServerUrl.getEditText().getText().toString();
        if (url.isEmpty()) {
            inputServerUrl.setError("Server URL cannot be empty!");
            return;
        }
        if (!url.startsWith("https://")) {
            url = "https://" + url;
        }
        if (!isValidUrl(url)) {
            inputServerUrl.setError("Please enter a Valid URL");
        } else {
            inputServerUrl.setError("");
            updateServerUrl(url);
        }
    }

    private boolean isValidUrl(String url) {
        if (url.contains(" ") || !url.equals(url.trim())) {
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

    private void updateServerUrl(String url) {
        PropertiesSingleton props = CommonToolProperties.get(requireActivity(), absSyncViewModel.getAppName());

        if (url.equals(absSyncViewModel.getUrl())) {
            Toast.makeText(requireActivity(), "No Change in the Server URL", Toast.LENGTH_SHORT).show();
            return;
        }

        props.setProperties(getUpdateUrlProperties(url));
        promptToVerifyServer();
    }

    public static Map<String,String> getUpdateUrlProperties(String url){
        Map<String, String> properties = new HashMap<>();
        properties.put(CommonToolProperties.KEY_SYNC_SERVER_URL, url);
        properties.put(CommonToolProperties.KEY_USERNAME, "");
        properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(false));
        properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.LOGGED_OUT.name());
        properties.put(CommonToolProperties.KEY_LAST_SYNC_INFO, null);
        properties.put(CommonToolProperties.KEY_IS_SERVER_VERIFIED, Boolean.toString(false));
        properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(false));
        properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED, null);
        properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
        properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
        properties.put(CommonToolProperties.KEY_USERS_LIST, "");
        return properties;
    }

    private void promptToVerifyServer(){
        /**
         * New dialog styling
         * MaterialAlertDialogBuilder is standard for all ODK-X Apps
         * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
         * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
         */

        AlertDialog alertDialog = new MaterialAlertDialogBuilder(requireActivity(),R.style.OdkXAlertDialogStyle)
                .setTitle("Server Settings Updated Successfully")
                .setMessage("Would you like to verify the Server now?")
                .setPositiveButton("Yes", (dialog, which) -> startVerifyActivity())
                .setNegativeButton("No", (dialog, which) -> navController.popBackStack())
                .setCancelable(false)
                .create();
        alertDialog.setCanceledOnTouchOutside(false);
        alertDialog.show();
    }

    private void startQrCodeScan(){
        // When Scan QR icon is clicked.
        if (checkSelfPermission(requireActivity(), Manifest.permission.CAMERA)
                == PackageManager.PERMISSION_GRANTED) {
            // Permission is already available, start camera preview
            openBarcodeScanner();
        } else {
            // Permission is missing and must be requested.
            requestCameraPermission();
        }
    }

    private void openBarcodeScanner() {
        IntentIntegrator
                .forSupportFragment(this)
                .setDesiredBarcodeFormats(IntentIntegrator.QR_CODE_TYPES)
                .setPrompt(getString(R.string.qr_code_scanner_instruction))
                .setCameraId(0)
                .setBeepEnabled(true)
                .setBarcodeImageEnabled(false)
                .initiateScan();
    }

    @Override
    public void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {
        IntentResult result = IntentIntegrator.parseActivityResult(requestCode, resultCode, data);
        if (result != null) {
            if (result.getContents() == null) {
                Toast.makeText(getActivity(), R.string.scanning_cancelled, Toast.LENGTH_SHORT).show();
            } else {
                parseQrCodeResult(result.getContents());
                Log.i("QR code:",result.getContents());
            }
        }
        super.onActivityResult(requestCode, resultCode, data);
    }

    private void parseQrCodeResult(String contents) {
        String TAG =  "pasreQrCodeResult";
        Map<String,String> properties=new HashMap<>();
        try {
            JSONObject mainObject = new JSONObject(contents);
            try{
                String url = (String)mainObject.get("url");
                properties.putAll(getUpdateUrlProperties(url));
            }
            catch (Exception e) {
                Log.i(TAG,"Url not found");
            }
            try{
                String username = (String) mainObject.get("username");
                //TODO - With Username
            }
            catch (Exception e) {
                //TODO - With Anonymous
                Log.i(TAG,"Username not found");
            }

            try{
                String password = (String) mainObject.get("password");
                //TODO - Password
            }
            catch (Exception e) {
                Log.i(TAG,"Password not found");
            }
        } catch (JSONException e) {
            Toast.makeText(getActivity(), R.string.invalid_qr_code, Toast.LENGTH_SHORT).show();
            Log.i(TAG,"Invalid Qr code");
        }

        PropertiesSingleton props = CommonToolProperties.get(requireActivity(), absSyncViewModel.getAppName());
        props.setProperties(properties);
        promptToVerifyServer();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions,
                                           @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == PERMISSION_REQUEST_CAMERA_CODE) {

            if (grantResults.length == 1 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                // Permission has been granted. Starting Barcode Scanner.
                openBarcodeScanner();
            }
            else{

                /**
                 * New dialog styling
                 * MaterialAlertDialogBuilder is standard for all ODK-X Apps
                 * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
                 * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
                 */

                MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity(),R.style.OdkXAlertDialogStyle);
                builder.setMessage(R.string.camera_permission_rationale)
                        .setPositiveButton(R.string.allow, (dialog, which) -> {
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                                //For pre Marshmallow devices, this wouldn't be called as they don't need runtime permission.
                                requestPermissions(
                                        new String[]{Manifest.permission.CAMERA},
                                        PERMISSION_REQUEST_CAMERA_CODE);
                            }
                        })
                        .setNegativeButton(R.string.not_now, (dialog, which) -> {
                            Toast.makeText(getActivity(), R.string.permission_denied, Toast.LENGTH_SHORT).show();
                            dialog.dismiss();
                        });
                builder.create().show();
            }
        }
    }
    private void requestCameraPermission() {

        if (checkSelfPermission(requireActivity(), Manifest.permission.CAMERA)
                != PackageManager.PERMISSION_GRANTED){
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                //For pre Marshmallow devices, this wouldn't be called as they don't need runtime permission.
                requestPermissions(
                        CAMERA_PERMISSION,
                        PERMISSION_REQUEST_CAMERA_CODE
                );
            }
        }
        else{
            // Permission has been granted. Starting Barcode Scanner.
            openBarcodeScanner();
        }
    }
}