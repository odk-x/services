package org.opendatakit.services.sync.actions.fragments;

import android.graphics.Paint;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.lifecycle.Lifecycle;
import androidx.navigation.Navigation;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;
import com.google.android.material.textfield.TextInputLayout;

import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.services.R;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.UserState;

import java.util.HashMap;
import java.util.Map;

public class SetCredentialsFragment extends LoginFragment {

    private class OnButtonClick implements View.OnClickListener {

        @Override
        public void onClick(View v) {
            if (v.getId() == R.id.btnAuthenticateUserLogin)
                signInUsingCredentials();
        }
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_set_credentials, container, false);
    }

    private TextView tvServerUrl, tvTitle;
    private TextInputLayout inputUsername, inputPassword;
    private Button btnSignIn;

    private String btnSignInText, titleText;
    private boolean btnSignInEnable;

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        findViewsAndAttachListeners(view);
        setupViewModelAndNavController();
    }

    private void findViewsAndAttachListeners(View view) {
        tvServerUrl = view.findViewById(R.id.tvServerUrlLogin);
        tvTitle = view.findViewById(R.id.tvTitleLogin);

        inputUsername = view.findViewById(R.id.inputUsernameLogin);
        inputPassword = view.findViewById(R.id.inputPasswordLogin);

        btnSignIn = view.findViewById(R.id.btnAuthenticateUserLogin);

        btnSignIn.setOnClickListener(new OnButtonClick());

        titleText = getString(R.string.drawer_sign_in_button_text);
        btnSignInText = getString(R.string.drawer_sign_in_button_text);
        btnSignInEnable = true;
        updateUserInterface();
    }

    @Override
    protected void setupViewModelAndNavController() {
        super.setupViewModelAndNavController();

        loginViewModel.getServerUrl().observe(getViewLifecycleOwner(), s -> {
            tvServerUrl.setText(s);
            tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
        });

        loginViewModel.getUsername().observe(getViewLifecycleOwner(), s -> {
            if (inputUsername.getEditText() != null)
                inputUsername.getEditText().setText(s);
        });

        loginViewModel.getFunctionType().observe(getViewLifecycleOwner(), s -> {
            switch (s) {
                case (Constants.LOGIN_TYPE_SIGN_IN): {
                    switch (loginViewModel.getUserState()) {
                        case LOGGED_OUT: {
                            inTypeSignInStateLoggedOut();
                            break;
                        }
                        case ANONYMOUS: {
                            inTypeSignInStateAnonymous();
                            break;
                        }
                        case AUTHENTICATED_USER: {
                            inTypeSignInStateAuthenticated();
                            break;
                        }
                    }
                    break;
                }
                case (Constants.LOGIN_TYPE_UPDATE_CREDENTIALS):
                    switch (loginViewModel.getUserState()) {
                        case LOGGED_OUT: {
                            inTypeUpdateCredentialsStateLoggedOut();
                            break;
                        }
                        case ANONYMOUS: {
                            inTypeUpdateCredentialsStateAnonymous();
                            break;
                        }
                        case AUTHENTICATED_USER: {
                            inTypeUpdateCredentialsStateAuthenticated();
                            break;
                        }
                    }
                    break;
                case (Constants.LOGIN_TYPE_SWITCH_SIGN_IN_TYPE):
                    switch (loginViewModel.getUserState()) {
                        case LOGGED_OUT: {
                            inTypeSwitchMethodStateLoggedOut();
                            break;
                        }
                        case ANONYMOUS: {
                            inTypeSwitchMethodStateAnonymous();
                            break;
                        }
                        case AUTHENTICATED_USER: {
                            inTypeSwitchMethodStateAuthenticated();
                            break;
                        }
                    }
                    break;
            }
            updateUserInterface();
        });
    }

    private void signInUsingCredentials() {
        String username, password;

        if (inputUsername.getEditText() != null)
            username = inputUsername.getEditText().getText().toString();
        else
            username = "";

        if (inputPassword.getEditText() != null)
            password = inputPassword.getEditText().getText().toString();
        else
            password = "";

        if (username.isEmpty() || password.isEmpty())
            Toast.makeText(requireActivity(), "Please Enter the Required Credentials", Toast.LENGTH_SHORT).show();
        else {
            getProps().setProperties(getCredentialsProperty(username, password));
            promptToVerifyUser();
        }
    }

    private void inTypeSignInStateLoggedOut() {
        updateViewsProperties(getString(R.string.drawer_sign_in_button_text),
                getString(R.string.drawer_sign_in_button_text), true);
    }

    private void inTypeSwitchMethodStateLoggedOut() {
        showToast("User is Logged Out!");
        updateViewsProperties(getString(R.string.switch_sign_in_type),
                getString(R.string.drawer_sign_in_button_text), false);
    }

    private void inTypeUpdateCredentialsStateLoggedOut() {
        showToast("User is Logged Out!");
        updateViewsProperties(getString(R.string.drawer_item_update_credentials),
                getString(R.string.set_credentials), true);
    }

    private void inTypeSignInStateAnonymous() {
        showToast("User is already Anonymous");
        updateViewsProperties(getString(R.string.drawer_sign_in_button_text),
                getString(R.string.sign_in_using_credentials), true);
    }

    private void inTypeSwitchMethodStateAnonymous() {
        updateViewsProperties(getString(R.string.switch_sign_in_type),
                getString(R.string.sign_in_using_credentials), true);
    }

    private void inTypeUpdateCredentialsStateAnonymous() {
        showToast("User is already Anonymous");
        updateViewsProperties(getString(R.string.drawer_item_update_credentials),
                getString(R.string.sign_in_using_credentials), true);
    }

    private void inTypeSignInStateAuthenticated() {
        showToast("User is already signed-in as an Authenticated User");
        updateViewsProperties(getString(R.string.drawer_sign_in_button_text),
                getString(R.string.drawer_item_update_credentials), true);
    }

    private void inTypeSwitchMethodStateAuthenticated() {
        showToast("User is already signed-in as an Authenticated User");
        updateViewsProperties(getString(R.string.switch_sign_in_type),
                getString(R.string.set_credentials), false);
    }

    private void inTypeUpdateCredentialsStateAuthenticated() {
        updateViewsProperties(getString(R.string.drawer_item_update_credentials),
                getString(R.string.drawer_item_update_credentials), true);
    }

    private void updateViewsProperties(String title, String btnText, boolean btnEnable) {
        titleText = title;
        btnSignInText = btnText;
        btnSignInEnable = btnEnable;
    }

    private void updateUserInterface() {
        tvTitle.setText(titleText);
        btnSignIn.setText(btnSignInText);
        perhapsEnableButtons();
    }

    public static Map<String, String> getCredentialsProperty(String username, String pw) {
        Map<String, String> properties = new HashMap<>();
        properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE, "username_password");
        properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.AUTHENTICATED_USER.name());
        properties.put(CommonToolProperties.KEY_USERNAME, username);
        properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(false));
        properties.put(CommonToolProperties.KEY_LAST_SYNC_INFO, null);
        properties.put(CommonToolProperties.KEY_PASSWORD, pw);
        properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
        properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
        properties.put(CommonToolProperties.KEY_USERS_LIST, "");
        return properties;
    }

    @Override
    void disableButtons() {
        if (getLifecycle().getCurrentState().isAtLeast(Lifecycle.State.CREATED))
            btnSignIn.setEnabled(false);
    }

    @Override
    void perhapsEnableButtons() {
        if (getLifecycle().getCurrentState().isAtLeast(Lifecycle.State.CREATED))
            btnSignIn.setEnabled(btnSignInEnable);
    }

    private void promptToVerifyUser() {
        /**
         * New dialog styling
         * MaterialAlertDialogBuilder is standard for all ODK-X Apps
         * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
         * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
         */

        AlertDialog alertDialog = new MaterialAlertDialogBuilder(requireActivity(),R.style.OdkXAlertDialogStyle)
                .setTitle("User Logged in Successfully")
                .setMessage("Would you like to verify the User now?")
                .setPositiveButton("Yes", (dialog, which) -> verifyServerSettings())
                .setNegativeButton("No", (dialog, which) -> requireActivity().finish())
                .setCancelable(false)
                .create();
        alertDialog.setCanceledOnTouchOutside(false);
        alertDialog.show();
    }

    private void showToast(String message) {
        Toast.makeText(requireActivity(), message, Toast.LENGTH_LONG).show();
    }

}