let loginUserInput = document.getElementById("loginUser");
let loginPasswordInput = document.getElementById("loginPassword");
let loginBtn = document.getElementById("loginBtn");
let ssoBtn = document.getElementById("ssoBtn");
let error = document.getElementById("error");
let serviceName = document.getElementById("serviceName");

function signIn() {
    let userName = loginUserInput.value;
    let loginPassword = loginPasswordInput.value;
    const authCred = window.btoa(`${userName}:${loginPassword}`);
    const req = new Request(window.location.origin, {
        credentials: 'include',
        headers: {
            'Content-Type': 'text/plain',
            'Authorization': `Basic ${authCred}`,
            'Request-Type': "Authentication",
        },
    });
    fetch(req).then((response) => {
        try {
            if (!response.ok) {
                error.textContent = "Invalid username or password"
                error.style.color = "red"
                throw new Error(`Response status: ${response.status}`);
            }
            error.textContent = ""
            window.location.href = window.location.origin
        } catch (error) {
            console.error("Error:", error);
        }
    });
}

function ssoProceed() {
    fetch(window.location.origin + "?action=initSSO", {
        headers: {
            'Content-Type': 'application/json'
        },
    }).then((response) => {
        if (!response.ok) {
            throw new Error(`Response status: ${response.status}`);
        }
        response.json().then(data => {
            console.log(data.loginURL);
            window.location.href = data.loginURL;
        })
    });
}

function checkRedirect() {
    const req = new Request(window.location.origin, {});
    fetch(req).then((response) => {
        if (response.redirected && !response.url.startsWith(window.location.origin + "/login")) {
            window.location.href = response.url;
        }
    });
}

function checkCheckSsoButton() {
    fetch(window.location.origin + "?action=ssoEnable", {}).then((response) => {
        if (!response.ok) {
            ssoBtn.disabled = true;
        } else {
            button.disabled = false;
        }
    });``
}

loginBtn.addEventListener("click", function () {
    checkRedirect()
    signIn();
});

loginPasswordInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") {
        signIn();
    }
});

loginUserInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") {
        signIn();
    }
});

ssoBtn.addEventListener("click", function () {
    ssoProceed();
});

window.addEventListener("DOMContentLoaded", (event) => {
    checkRedirect();
    checkCheckSsoButton();
    switch (window.location.port) {
        case "8088":
        case "8090":
            serviceName.textContent = "YARN Resource Manager";
            break;
        case "8042":
        case "8044":
            serviceName.textContent = "YARN Node Manager";
            break;
        case "19888":
        case "19890":
            serviceName.textContent = "YARN Job History Server";
            break;
        case "8188":
        case "8190":
            serviceName.textContent = "YARN Timeline Server";
            break;
        default:
            serviceName.textContent = "YARN service";
    }
});
