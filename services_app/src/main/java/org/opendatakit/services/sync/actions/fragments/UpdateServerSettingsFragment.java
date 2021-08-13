package org.opendatakit.services.sync.actions.fragments;

import android.content.Intent;
import android.os.Bundle;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

import com.google.android.material.textfield.TextInputLayout;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.UserState;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

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
                // TODO
            }
        }
    }

    private TextInputLayout inputServerUrl;
    private AbsSyncViewModel absSyncViewModel;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_update_server_settings, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        findViewsAndAttachListeners(view);
    }

    private void findViewsAndAttachListeners(View view) {
        absSyncViewModel = new ViewModelProvider(requireActivity()).get(AbsSyncViewModel.class);

        inputServerUrl = view.findViewById(R.id.inputServerUrl);
        Button btnUpdateUrl = view.findViewById(R.id.btnUpdateServerUrl);
        Button btnSetDefault = view.findViewById(R.id.btnChooseDefaultServer);
        Button btnVerifyServerDetails = view.findViewById(R.id.btnVerifyServerUpdateServerDetails);
        Button btnScanQr = view.findViewById(R.id.btnScanQrUpdateServerDetails);

        OnButtonClick onButtonClick = new OnButtonClick();
        btnUpdateUrl.setOnClickListener(onButtonClick);
        btnSetDefault.setOnClickListener(onButtonClick);
        btnVerifyServerDetails.setOnClickListener(onButtonClick);
        btnScanQr.setOnClickListener(onButtonClick);

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

        Map<String, String> properties = new HashMap<>();
        properties.put(CommonToolProperties.KEY_SYNC_SERVER_URL, url);
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

        Toast.makeText(requireActivity(), "Server URL Updated Successfully!", Toast.LENGTH_SHORT).show();
    }
}