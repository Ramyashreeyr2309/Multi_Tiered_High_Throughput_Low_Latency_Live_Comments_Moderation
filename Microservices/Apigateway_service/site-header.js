async function loadSiteHeader() {
  const placeholder = document.querySelector("#site-header");
  if (!placeholder) {
    throw new Error("Site header placeholder is missing.");
  }

  const response = await fetch("/media/site-header.html");
  if (!response.ok) {
    throw new Error(`Could not load the shared site header (${response.status}).`);
  }

  const template = document.createElement("template");
  template.innerHTML = await response.text();
  const style = template.content.querySelector("style");
  const header = template.content.querySelector("header.site-header");
  if (!style || !header) {
    throw new Error("The shared site header file is invalid.");
  }

  document.head.append(style);
  placeholder.replaceWith(header);

  const profile = header.querySelector("#site-profile");
  const profileTrigger = header.querySelector("#site-profile-trigger");
  const name = header.querySelector("#site-profile-name");
  const email = header.querySelector("#site-profile-email");
  const accountMenu = header.querySelector("#site-account-menu");
  const logoutButton = header.querySelector("#site-logout-button");
  const accessToken = sessionStorage.getItem("access_token");
  const mailId = sessionStorage.getItem("mail_id");

  if (!profile || !profileTrigger || !name || !email || !accountMenu || !logoutButton) {
    throw new Error("The shared site header profile controls are missing.");
  }

  if (accessToken && mailId) {
    name.textContent = sessionStorage.getItem("user_name") || mailId;
    email.textContent = mailId;
    profile.hidden = false;
  }

  profileTrigger.addEventListener("click", () => {
    accountMenu.hidden = !accountMenu.hidden;
    profileTrigger.setAttribute("aria-expanded", String(!accountMenu.hidden));
  });

  logoutButton.addEventListener("click", async () => {
    logoutButton.disabled = true;
    try {
      const currentAccessToken = sessionStorage.getItem("access_token");
      const currentMailId = sessionStorage.getItem("mail_id");
      if (currentAccessToken && currentMailId) {
        const response = await fetch(
          `/api/v1/user_service/user/session/current/?mail_id=${encodeURIComponent(currentMailId)}`,
          { method: "DELETE", headers: { Authorization: `Bearer ${currentAccessToken}` } }
        );
        if (!response.ok) {
          console.error(`Logout API returned ${response.status}.`);
        }
      }
    } catch (error) {
      console.error("Logout API request failed.", error);
    } finally {
      sessionStorage.removeItem("access_token");
      sessionStorage.removeItem("refresh_token");
      sessionStorage.removeItem("token_type");
      sessionStorage.removeItem("user_name");
      sessionStorage.removeItem("mail_id");
      window.location.replace("/login");
    }
  });
}

loadSiteHeader().catch((error) => {
  console.error("Failed to load the shared site header.", error);
});
